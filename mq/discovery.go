package mq

import (
	"context"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/memberlist"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
)

const (
	discoveryBufferSize = 65536
	udpReadBufferSize   = 2 * 1024 * 1024
)

type discoveryTransport struct {
	packetCh     chan *memberlist.Packet
	streamCh     chan net.Conn
	tcpListeners []*net.TCPListener
	udpListeners []*net.UDPConn
	shutdown     int32
	wg           sync.WaitGroup
}

func NewDiscoveryTransport(host string, port int) (transport *discoveryTransport, err error) {
	t := &discoveryTransport{
		packetCh: make(chan *memberlist.Packet),
		streamCh: make(chan net.Conn),
	}
	defer func() {
		if err != nil {
			t.Shutdown()
		}
	}()

	var ips []net.IP
	if host == "" {
		ips = []net.IP{nil}
	} else {
		ips, err = net.LookupIP(host)
		if err != nil {
			return nil, err
		}
	}

	for _, ip := range ips {
		addr := &net.UDPAddr{IP: ip, Port: port}
		udpListener, err := net.ListenUDP("udp", addr)
		if err != nil {
			return nil, errors.Wrapf(err, "could not start UDP on [%s:%d]", ip.String(), port)
		}
		if err := setUDPReadBuffer(udpListener); err != nil {
			return nil, errors.Wrap(err, "could not resize UDP read buffer")
		}
		t.udpListeners = append(t.udpListeners, udpListener)
	}

	for i := 0; i < len(ips); i++ {
		t.wg.Add(1)
		go t.listenUDP(t.udpListeners[i])
	}

	return t, nil
}

func (t *discoveryTransport) listenUDP(conn *net.UDPConn) {
	defer t.wg.Done()
	for {
		buf := make([]byte, discoveryBufferSize)
		n, addr, err := conn.ReadFrom(buf)
		ts := time.Now()
		if err != nil {
			if s := atomic.LoadInt32(&t.shutdown); s == 1 {
				break
			}
			continue
		}
		if n < 1 {
			continue
		}
		t.packetCh <- &memberlist.Packet{
			Buf:       buf[:n],
			From:      addr,
			Timestamp: ts,
		}
	}
}

func (t *discoveryTransport) FinalAdvertiseAddr(addr string, port int) (net.IP, int, error) {
	ip := net.ParseIP(addr)
	if ip == nil {
		return nil, 0, errors.Errorf("could not parse address: %q", addr)
	}
	return ip, port, nil
}

func (t *discoveryTransport) WriteTo(b []byte, addr string) (time.Time, error) {
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return time.Time{}, err
	}
	_, err = t.udpListeners[0].WriteTo(b, udpAddr)
	return time.Now(), err
}

func (t *discoveryTransport) PacketCh() <-chan *memberlist.Packet {
	return t.packetCh
}

func (t *discoveryTransport) DialTimeout(addr string, timeout time.Duration) (conn net.Conn, err error) {
	ctx, _ := context.WithTimeout(context.Background(), timeout)
	// TODO: connection pooling?
	// TODO: dial options?
	grpcConn, err := grpc.DialContext(ctx, addr, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			grpcConn.Close()
		}
	}()
	client := NewDiscoveryRPCClient(grpcConn)

	tunnel, err := client.Tunnel(context.Background())
	if err != nil {
		return nil, err
	}

	conn, pipe := net.Pipe()

	var pending int32 = 2

	go func() {
		defer func() {
			pipe.Close()
			if atomic.AddInt32(&pending, -1) == 0 {
				grpcConn.Close()
			}
		}()
		for {
			in, err := tunnel.Recv()
			if err != nil {
				return
			}
			_, err = pipe.Write(in.Data)
			if err != nil {
				return
			}
		}
	}()

	go func() {
		defer func() {
			tunnel.CloseSend()
			if atomic.AddInt32(&pending, -1) == 0 {
				grpcConn.Close()
			}
		}()
		buf := make([]byte, discoveryBufferSize)
		for {
			buf = buf[:cap(buf)]
			n, err := pipe.Read(buf)
			if err != nil {
				return
			}
			err = tunnel.Send(&TunnelData{Data: buf[:n]})
			if err != nil {
				return
			}
		}
	}()

	return conn, nil
}

func (t *discoveryTransport) StreamCh() <-chan net.Conn {
	return t.streamCh
}

func (t *discoveryTransport) Shutdown() error {
	atomic.StoreInt32(&t.shutdown, 1)

	for _, conn := range t.udpListeners {
		conn.Close()
	}

	t.wg.Wait()
	return nil
}

func setUDPReadBuffer(c *net.UDPConn) error {
	size := udpReadBufferSize
	var err error
	for size > 0 {
		if err = c.SetReadBuffer(size); err == nil {
			return nil
		}
		size = size / 2
	}
	return err
}

func (t *discoveryTransport) Tunnel(stream DiscoveryRPC_TunnelServer) error {
	conn, pipe := net.Pipe()
	defer pipe.Close()
	t.streamCh <- conn

	go func() {
		defer pipe.Close()
		buf := make([]byte, discoveryBufferSize)
		for {
			buf = buf[:cap(buf)]
			n, err := pipe.Read(buf)
			if err != nil {
				return
			}
			err = stream.Send(&TunnelData{Data: buf[:n]})
			if err != nil {
				return
			}
		}
	}()

	for {
		in, err := stream.Recv()
		if err == io.EOF {
			return nil
		} else if err != nil {
			return err
		}

		_, err = pipe.Write(in.Data)
		if err != nil {
			return nil
		}
	}
}
