package cmd

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"eventter.io/mq"
	"eventter.io/mq/about"
	"eventter.io/mq/amqp"
	"eventter.io/mq/amqp/sasl"
	"eventter.io/mq/emq"
	"eventter.io/mq/segments"
	"github.com/bbva/raft-badger"
	"github.com/hashicorp/memberlist"
	"github.com/hashicorp/raft"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
)

var rootConfig = &mq.Config{}

func newClient(ctx context.Context) (emq.Client, error) {
	return emq.DialContext(ctx, fmt.Sprintf("%s:%d", rootConfig.BindHost, rootConfig.Port), grpc.WithInsecure())
}

func Cmd() *cobra.Command {
	var join []string

	cmd := &cobra.Command{
		Use:     about.Name,
		Version: about.Version,
		PreRunE: func(cmd *cobra.Command, args []string) error {
			return rootConfig.Init()
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			rand.Seed(time.Now().UnixNano())

			advertiseIPs, err := net.LookupIP(rootConfig.AdvertiseHost)
			if err != nil {
				return errors.Wrap(err, "advertise host lookup failed")
			}
			var advertiseIP net.IP
			for _, candidateIp := range advertiseIPs {
				if ip4 := candidateIp.To4(); ip4 != nil {
					advertiseIP = ip4
					break
				}
			}
			if advertiseIP == nil {
				advertiseIP = advertiseIPs[0]
			}

			grpcServer := grpc.NewServer(grpc.MaxRecvMsgSize(64 * 1024 * 1024))

			clientPool := mq.NewClientConnPool(30*time.Second, grpc.WithInsecure())

			discoveryTransport, err := mq.NewDiscoveryRPCTransport(rootConfig.BindHost, rootConfig.Port, clientPool)
			if err != nil {
				return errors.Wrap(err, "discovery transport failed")
			}
			defer discoveryTransport.Shutdown()
			mq.RegisterDiscoveryRPCServer(grpcServer, discoveryTransport)

			raftTransport := mq.NewRaftRPCTransport(advertiseIP, rootConfig.Port, clientPool)
			mq.RegisterRaftRPCServer(grpcServer, raftTransport)

			nodeName := mq.NodeIDToString(rootConfig.ID)

			raftConfig := raft.DefaultConfig()
			raftConfig.LocalID = raft.ServerID(nodeName)

			raftDir := filepath.Join(rootConfig.Dir, "raft")
			if err := os.MkdirAll(raftDir, rootConfig.DirPerm); err != nil {
				return err
			}

			raftLogStore, err := raftbadger.NewBadgerStore(filepath.Join(raftDir, "logs"))
			if err != nil {
				return errors.Wrap(err, "could not open raft logs store")
			}
			defer raftLogStore.Close()

			raftStableStore, err := raftbadger.NewBadgerStore(filepath.Join(raftDir, "stable"))
			if err != nil {
				return errors.Wrap(err, "could not open raft stable store")
			}
			defer raftStableStore.Close()

			raftSnapshotStore, err := raft.NewFileSnapshotStore(raftDir, 2, os.Stdout)
			if err != nil {
				return errors.Wrap(err, "could not open raft snapshot store")
			}

			clusterState := mq.NewClusterStateStore()

			raftNode, err := raft.NewRaft(
				raftConfig,
				clusterState,
				raftLogStore,
				raftStableStore,
				raftSnapshotStore,
				raftTransport,
			)
			if err != nil {
				return errors.Wrap(err, "could not start raft node")
			}
			defer func() {
				raftNode.Shutdown().Error()
			}()

			segmentDir, err := segments.NewDir(
				filepath.Join(rootConfig.Dir, "segments"),
				rootConfig.DirPerm,
				0644,
				64*1024*1024, // 64 MiB
				60*time.Second,
			)
			if err != nil {
				return errors.Wrap(err, "could not open segments dir")
			}
			defer segmentDir.Close()

			listConfig := memberlist.DefaultLANConfig()
			listConfig.Name = nodeName
			listConfig.Transport = discoveryTransport
			listConfig.AdvertiseAddr = advertiseIP.String()
			listConfig.AdvertisePort = rootConfig.Port
			memberEventC := make(chan memberlist.NodeEvent, 128)
			listConfig.Events = &memberlist.ChannelEventDelegate{Ch: memberEventC}
			members, err := memberlist.Create(listConfig)
			if err != nil {
				return errors.Wrap(err, "could not start node discovery")
			}
			defer members.Shutdown()

			server := mq.NewServer(rootConfig.ID, members, raftNode, clientPool, clusterState, segmentDir)
			go server.Loop(memberEventC)
			defer server.Close()

			emq.RegisterEventterMQServer(grpcServer, server)
			mq.RegisterNodeRPCServer(grpcServer, server)

			grpcListener, err := net.Listen("tcp", rootConfig.BindHost+":"+strconv.Itoa(rootConfig.Port))
			if err != nil {
				return errors.Wrap(err, "grpc listen failed")
			}
			defer grpcListener.Close()
			go grpcServer.Serve(grpcListener)
			defer grpcServer.Stop()
			log.Println("grpc server started on", grpcListener.Addr())

			if len(join) == 0 {
				hasExistingState, err := raft.HasExistingState(raftLogStore, raftStableStore, raftSnapshotStore)
				if err != nil {
					return err
				}
				if !hasExistingState {
					var servers []raft.Server
					for _, node := range members.Members() {
						servers = append(servers, raft.Server{
							ID:      raft.ServerID(node.Name),
							Address: raft.ServerAddress(node.Addr.String() + ":" + strconv.Itoa(int(node.Port))),
						})
					}

					future := raftNode.BootstrapCluster(raft.Configuration{Servers: servers})
					if err := future.Error(); err != nil {
						return errors.Wrap(err, "bootstrap failed")
					}

					log.Printf("cluster bootstrapped")
				}
			} else {
				_, err = members.Join(join)
				if err != nil {
					return errors.Wrap(err, "join failed")
				}
			}

			amqpListener, err := net.Listen("tcp", rootConfig.BindHost+":"+strconv.Itoa(rootConfig.AMQPPort))
			if err != nil {
				return errors.Wrap(err, "amqp listen failed")
			}
			defer amqpListener.Close()
			allowAll := func(username, password string) (bool, error) {
				return true, nil
			}
			amqpServer := &amqp.Server{
				Name:           about.Name,
				Version:        about.Version,
				CapabilitiesV0: []string{"basic.nack"},
				HandlerV0:      server,
				HandlerV1:      server,
				SASLProviders: []sasl.Provider{
					sasl.NewPLAIN(allowAll),
					sasl.NewAMQPLAIN(allowAll),
				},
			}
			go amqpServer.Serve(amqpListener)
			defer amqpServer.Close()
			log.Println("amqp server started at", amqpListener.Addr())

			interrupt := make(chan os.Signal, 1)
			signal.Notify(interrupt, syscall.SIGINT, syscall.SIGTERM)
			<-interrupt

			log.Print("gracefully shutting down")

			gracefulShutdownTimeout := 10 * time.Second
			gracefulShutdownCtx, gracefulShutdownCancel := context.WithTimeout(context.Background(), gracefulShutdownTimeout)

			go func() {
				select {
				case <-interrupt:
					gracefulShutdownCancel()
				case <-gracefulShutdownCtx.Done():
					return
				}
			}()

			go func() {
				grpcServer.GracefulStop()
				gracefulShutdownCancel()
			}()

			<-gracefulShutdownCtx.Done()

			return nil
		},
	}

	cmd.PersistentFlags().StringVar(&rootConfig.BindHost, "host", "", "Node host.")
	cmd.PersistentFlags().IntVar(&rootConfig.Port, "port", 16000, "Node port.")
	cmd.Flags().Uint64Var(&rootConfig.ID, "id", 0, "Node ID. Must be unique across cluster & stable.")
	cmd.Flags().StringVar(&rootConfig.AdvertiseHost, "advertise-host", "", "Host that will the node advertise to others.")
	cmd.Flags().IntVar(&rootConfig.AMQPPort, "amqp-port", 0, "AMQP port. If not specified, defaults to `port + 1`.")
	cmd.Flags().StringVar(&rootConfig.Dir, "dir", "", "Persistent data directory.")
	cmd.Flags().Uint32Var((*uint32)(&rootConfig.DirPerm), "dir-perm", 0755, "Persistent data directory permissions.")
	cmd.Flags().StringSliceVar(&join, "join", nil, "Running peers to join.")

	cmd.AddCommand(
		createConsumerGroupCmd(),
		createNamespaceCmd(),
		createTopicCmd(),
		debugCmd(),
		deleteConsumerGroupCmd(),
		deleteNamespaceCmd(),
		deleteTopicCmd(),
		listConsumerGroupsCmd(),
		listTopicsCmd(),
		publishCmd(),
		subscribeCmd(),
	)

	return cmd
}
