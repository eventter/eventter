package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"eventter.io/mq"
	"eventter.io/mq/client"
	"eventter.io/mq/segmentfile"
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

			segmentDir, err := segmentfile.NewDir(
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

			client.RegisterEventterMQServer(grpcServer, server)
			mq.RegisterNodeRPCServer(grpcServer, server)

			listener, err := net.Listen("tcp", rootConfig.BindHost+":"+strconv.Itoa(rootConfig.Port))
			if err != nil {
				return errors.Wrap(err, "listen failed")
			}
			defer listener.Close()
			go grpcServer.Serve(listener)
			defer grpcServer.Stop()

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

			interrupt := make(chan os.Signal, 1)
			signal.Notify(interrupt, syscall.SIGINT, syscall.SIGTERM)
			<-interrupt

			log.Print("gracefully shutting down")

			gracefulShutdownTimeout := 10 * time.Second
			gracefulShutdownCtx, gracefulShutdownDone := context.WithTimeout(context.Background(), gracefulShutdownTimeout)

			go func() {
				grpcServer.GracefulStop()
				gracefulShutdownDone()
			}()

			<-gracefulShutdownCtx.Done()

			if gracefulShutdownCtx.Err() == context.DeadlineExceeded {
				log.Printf("graceful shutdown did not complete in %s, forcefully shutting down", gracefulShutdownTimeout.String())
			}

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
		debugCmd(),
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
