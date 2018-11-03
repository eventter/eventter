package mq

import (
	"context"
	"sync"
	"time"

	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
)

type ClientConnPool struct {
	idleTimeout     time.Duration
	dialOptions     []grpc.DialOption
	mutex           sync.Mutex
	clients         map[string]*grpc.ClientConn
	targets         map[*grpc.ClientConn]string
	referenceCounts map[*grpc.ClientConn]int
	idlingSince     map[*grpc.ClientConn]time.Time
	closeC          chan struct{}
}

func NewClientConnPool(idleTimeout time.Duration, dialOptions ...grpc.DialOption) *ClientConnPool {
	if idleTimeout < 0 {
		panic("idle timeout must not be negative")
	}

	p := &ClientConnPool{
		idleTimeout:     idleTimeout,
		dialOptions:     dialOptions,
		clients:         make(map[string]*grpc.ClientConn),
		targets:         make(map[*grpc.ClientConn]string),
		referenceCounts: make(map[*grpc.ClientConn]int),
		idlingSince:     make(map[*grpc.ClientConn]time.Time),
		closeC:          make(chan struct{}),
	}

	if idleTimeout > 0 {
		go p.closeIdle()
	}

	return p
}

func (p *ClientConnPool) closeIdle() {
	ticker := time.NewTicker(p.idleTimeout)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			p.mutex.Lock()
			t := time.Now().Add(-p.idleTimeout)
			var clients []*grpc.ClientConn
			for client, since := range p.idlingSince {
				if since.Before(t) {
					clients = append(clients, client)
				}
			}
			for _, client := range clients {
				p.closeClient(client)
			}
			p.mutex.Unlock()
		case <-p.closeC:
			return
		}
	}
}

func (p *ClientConnPool) Get(ctx context.Context, target string) (*grpc.ClientConn, error) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if client, ok := p.clients[target]; ok {
		p.referenceCounts[client] += 1
		delete(p.idlingSince, client)
		return client, nil
	}

	client, err := grpc.DialContext(ctx, target, p.dialOptions...)

	if err != nil {
		return nil, err
	}

	p.clients[target] = client
	p.targets[client] = target
	p.referenceCounts[client] += 1
	delete(p.idlingSince, client)

	return client, nil
}

func (p *ClientConnPool) Put(client *grpc.ClientConn) error {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	rc, ok := p.referenceCounts[client]
	if !ok {
		return errors.New("connection does not belong to this pool")
	}

	rc -= 1

	if rc > 0 {
		p.referenceCounts[client] = rc
		return nil

	} else if p.idleTimeout > 0 && (client.GetState() == connectivity.Idle || client.GetState() == connectivity.Ready) {
		p.referenceCounts[client] = rc
		p.idlingSince[client] = time.Now()
		return nil

	} else {
		return p.closeClient(client)
	}
}

func (p *ClientConnPool) closeClient(client *grpc.ClientConn) error {
	target := p.targets[client]
	delete(p.clients, target)
	delete(p.targets, client)
	delete(p.referenceCounts, client)
	delete(p.idlingSince, client)
	return client.Close()
}

func (p *ClientConnPool) Close() {
	close(p.closeC)
}
