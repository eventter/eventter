package main

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"eventter.io/livereload"
	"github.com/spf13/cobra"
)

type server struct {
	mu    sync.Mutex
	conns map[net.Conn]struct{}
	wg    sync.WaitGroup
}

func main() {
	var live string

	var rootCmd = &cobra.Command{
		Use: os.Args[0],
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			master, worker, err := livereload.New(&livereload.Config{
				Live:      live != "",
				Address:   live,
				Package:   "eventter.io/cmd/livereload-example",
				Listeners: []livereload.ListenerDefinition{{Network: "tcp", Address: ":8080"}},
			})
			if err != nil {
				return err
			}

			// This is a master process.
			if master != nil {
				return master.Run()
			}

			// If `master` is nil, it means this is worker process. Create listener and do work as usual.
			listener, err := worker.Listen(0)
			if err != nil {
				return err
			}
			defer listener.Close()

			ctx, cancel := context.WithCancel(context.Background())
			server := &server{
				conns: make(map[net.Conn]struct{}),
				wg:    sync.WaitGroup{},
			}
			defer func() {
				if err == nil {
					server.wg.Wait()
					if ctx.Err() == context.Canceled {
						log.Println("shutdown complete")
					}
				}
			}()

			interrupt := make(chan os.Signal, 1)
			signal.Notify(interrupt, syscall.SIGINT, syscall.SIGTERM)
			go func() {
				<-interrupt
				log.Println("gracefully shutting down")
				cancel()
				listener.Close()
				server.mu.Lock()
				defer server.mu.Unlock()

				t := time.Unix(0, 0)
				for conn, _ := range server.conns {
					if err := conn.SetDeadline(t); err != nil {
						log.Printf("could not set read deadline [%s]: %s", conn.RemoteAddr(), err)
					}
				}
			}()

			log.Printf("ready to accept connections on [%s]", listener.Addr())

			if err := worker.Ready(); err != nil {
				return err
			}

			for {
				conn, err := listener.Accept()
				if err != nil {
					if ctx.Err() == context.Canceled {
						return nil
					}
					return err
				}

				go server.handle(ctx, conn)
			}
		},
	}

	rootCmd.PersistentFlags().StringVar(&live, "live", "", "Path to livereload socket.")

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func (s *server) handle(ctx context.Context, conn net.Conn) {
	s.wg.Add(1)
	defer s.wg.Done()

	s.mu.Lock()
	s.conns[conn] = struct{}{}
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		delete(s.conns, conn)

		if err := conn.Close(); err != nil {
			log.Printf("error while closing [%s]: %s", conn.RemoteAddr(), err)
		}
	}()

	log.Printf("accepted connection [%s]", conn.RemoteAddr())

	buf := bufio.NewReader(conn)
	for {
		if ctx.Err() == context.Canceled {
			log.Printf("context cancelled, closing connection [%s]", conn.RemoteAddr())
			return
		}

		if err := conn.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
			log.Printf("could not set read deadline for [%s]: %s", conn.RemoteAddr(), err)
			return
		}
		line, err := buf.ReadBytes('\n')
		if err != nil {
			if err == io.EOF || ctx.Err() == context.Canceled {
				log.Printf("closing connection [%s]", conn.RemoteAddr())
			} else {
				log.Printf("could read from [%s]: %s", conn.RemoteAddr(), err)
			}
			return
		}

		if err := conn.SetWriteDeadline(time.Now().Add(1 * time.Second)); err != nil {
			log.Printf("could not set write deadline for [%s]: %s", conn.RemoteAddr(), err)
			return
		}
		if _, err := conn.Write(line); err != nil {
			log.Printf("could write to [%s]: %s", conn.RemoteAddr(), err)
			return
		}

		log.Printf("echoed message [%s] to [%s]", bytes.TrimRight(line, "\r\n"), conn.RemoteAddr())
	}
}
