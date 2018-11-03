package main

import (
	"fmt"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"eventter.io/mq"
	"eventter.io/mq/client"
	"github.com/bbva/raft-badger"
	"github.com/hashicorp/memberlist"
	"github.com/hashicorp/raft"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
)

func rootCmd() *cobra.Command {
	var join []string

	cmd := &cobra.Command{
		Use: os.Args[0],
		PreRunE: func(cmd *cobra.Command, args []string) error {
			return rootConfig.Init()
		},
		RunE: func(cmd *cobra.Command, args []string) error {
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

			grpcServer := grpc.NewServer()

			clientPool := mq.NewClientConnPool(30*time.Second, grpc.WithInsecure())

			discoveryTransport, err := mq.NewDiscoveryRPCTransport(rootConfig.BindHost, rootConfig.Port, clientPool)
			if err != nil {
				return errors.Wrap(err, "discovery transport failed")
			}
			defer discoveryTransport.Shutdown()
			mq.RegisterDiscoveryRPCServer(grpcServer, discoveryTransport)

			raftTransport := mq.NewRaftRPCTransport(advertiseIP, rootConfig.Port, clientPool)
			mq.RegisterRaftRPCServer(grpcServer, raftTransport)

			listener, err := net.Listen("tcp", rootConfig.BindHost+":"+strconv.Itoa(rootConfig.Port))
			if err != nil {
				return errors.Wrap(err, "listen failed")
			}
			defer listener.Close()

			nodeName := fmt.Sprintf("%016x", rootConfig.ID)

			raftConfig := raft.DefaultConfig()
			raftConfig.LocalID = raft.ServerID(nodeName)

			raftDir := filepath.Join(rootConfig.Dir, "raft")
			if err := os.MkdirAll(raftDir, rootConfig.DirPerm); err != nil {
				return err
			}

			raftLogStore, err := raftbadger.NewBadgerStore(filepath.Join(raftDir, "logs"))
			if err != nil {
				return errors.Wrap(err, "could not open raft store")
			}
			defer raftLogStore.Close()

			raftStableStore, err := raftbadger.NewBadgerStore(filepath.Join(raftDir, "stable"))
			if err != nil {
				return errors.Wrap(err, "could not open raft store")
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

			server := mq.NewServer(raftNode, clientPool, clusterState)
			client.RegisterEventterMQServer(grpcServer, server)

			go grpcServer.Serve(listener)
			defer grpcServer.Stop()

			listConfig := memberlist.DefaultLANConfig()
			listConfig.Name = nodeName
			listConfig.Transport = discoveryTransport
			listConfig.AdvertiseAddr = advertiseIP.String()
			listConfig.AdvertisePort = rootConfig.Port
			listConfig.Events = mq.NewRaftMembersDelegate(raftNode)
			list, err := memberlist.Create(listConfig)
			if err != nil {
				return errors.Wrap(err, "could not start discovery")
			}
			defer list.Shutdown()

			if len(join) == 0 {
				needsBootstrap, err := raft.HasExistingState(raftLogStore, raftStableStore, raftSnapshotStore)
				if err != nil {
					return err
				}
				if !needsBootstrap {
					var servers []raft.Server
					for _, node := range list.Members() {
						servers = append(servers, raft.Server{
							ID:      raft.ServerID(node.Name),
							Address: raft.ServerAddress(node.Addr.String() + ":" + strconv.Itoa(int(node.Port))),
						})
					}

					future := raftNode.BootstrapCluster(raft.Configuration{Servers: servers})
					if err := future.Error(); err != nil {
						return errors.Wrap(err, "could not bootstrap raft")
					}

					fmt.Println("bootstrapped")
				}
			} else {
				_, err = list.Join(join)
				if err != nil {
					return errors.Wrap(err, "could not join discovery")
				}
			}

			interrupt := make(chan os.Signal, 1)
			signal.Notify(interrupt, syscall.SIGINT, syscall.SIGTERM)
			<-interrupt

			grpcServer.GracefulStop()

			return nil
		},
	}

	cmd.PersistentFlags().StringVar(&rootConfig.BindHost, "host", "", "Node host.")
	cmd.PersistentFlags().IntVar(&rootConfig.Port, "port", 16000, "Node port.")
	cmd.Flags().Uint64Var(&rootConfig.ID, "id", 0, "Node ID. Must be unique across cluster & stable.")
	cmd.Flags().StringVar(&rootConfig.AdvertiseHost, "advertise-host", "", "Host that will the node advertise to others.")
	cmd.Flags().StringVar(&rootConfig.Dir, "dir", "", "Persistent data directory.")
	cmd.Flags().Uint32Var((*uint32)(&rootConfig.DirPerm), "dir-perm", 0755, "Persistent data directory permissions.")
	cmd.Flags().StringSliceVar(&join, "join", nil, "Running peers to join.")

	cmd.AddCommand(
		configureConsumerGroupCmd(),
		configureTopicCmd(),
		consumeCmd(),
		deleteConsumerGroupCmd(),
		deleteTopicCmd(),
		listConsumerGroupsCmd(),
		listTopicsCmd(),
		publishCmd(),
	)

	return cmd
}
