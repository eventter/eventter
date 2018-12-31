package mq

import (
	"bytes"
	"context"
	"io"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
)

const (
	installSnapshotBufferSize = 65536
)

type RaftRPCTransport struct {
	advertiseIP      net.IP
	advertisePort    int
	pool             *ClientConnPool
	ch               chan raft.RPC
	mutex            sync.RWMutex
	heartbeatHandler func(rpc raft.RPC)
}

func NewRaftRPCTransport(advertiseIP net.IP, advertisePort int, pool *ClientConnPool) *RaftRPCTransport {
	return &RaftRPCTransport{
		advertiseIP:   advertiseIP,
		advertisePort: advertisePort,
		pool:          pool,
		ch:            make(chan raft.RPC, 128),
	}
}

func (t *RaftRPCTransport) Consumer() <-chan raft.RPC {
	return t.ch
}

func (t *RaftRPCTransport) LocalAddr() raft.ServerAddress {
	return raft.ServerAddress(t.advertiseIP.String() + ":" + strconv.Itoa(t.advertisePort))
}

func (t *RaftRPCTransport) AppendEntriesPipeline(id raft.ServerID, target raft.ServerAddress) (pipeline raft.AppendPipeline, err error) {
	conn, err := t.pool.Get(context.Background(), string(target))
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			t.pool.Put(conn)
		}
	}()

	client := NewRaftRPCClient(conn)

	stream, err := client.DoAppendEntries(context.Background())
	if err != nil {
		return nil, err
	}

	return &raftRPCTransportAppendPipeline{
		pool:   t.pool,
		conn:   conn,
		stream: stream,
		ch:     make(chan raft.AppendFuture),
	}, nil
}

type raftRPCTransportAppendPipeline struct {
	pool   *ClientConnPool
	conn   *grpc.ClientConn
	stream RaftRPC_DoAppendEntriesClient
	ch     chan raft.AppendFuture
}

func (p *raftRPCTransportAppendPipeline) AppendEntries(request *raft.AppendEntriesRequest, response *raft.AppendEntriesResponse) (raft.AppendFuture, error) {
	start := time.Now()

	var entries []*AppendEntriesRequest_Entry
	for _, entry := range request.Entries {
		entries = append(entries, &AppendEntriesRequest_Entry{
			Index: entry.Index,
			Term:  entry.Term,
			Type:  AppendEntriesRequest_Entry_Type(entry.Type),
			Data:  entry.Data,
		})
	}
	err := p.stream.Send(&AppendEntriesRequest{
		Term:              request.Term,
		Leader:            request.Leader,
		PrevLogEntry:      request.PrevLogEntry,
		PrevLogTerm:       request.PrevLogTerm,
		Entries:           entries,
		LeaderCommitIndex: request.LeaderCommitIndex,
	})
	if err != nil {
		return nil, err
	}

	rpcResponse, err := p.stream.Recv()
	if err != nil {
		return nil, err
	}

	*response = raft.AppendEntriesResponse{
		Term:           rpcResponse.Term,
		LastLog:        rpcResponse.LastLog,
		Success:        rpcResponse.Success,
		NoRetryBackoff: rpcResponse.NoRetryBackoff,
	}

	p.ch <- &raftRPCTransportAppendFuture{
		start:    start,
		request:  request,
		response: response,
	}

	return nil, nil
}

func (p *raftRPCTransportAppendPipeline) Consumer() <-chan raft.AppendFuture {
	return p.ch
}

func (p *raftRPCTransportAppendPipeline) Close() error {
	p.stream.CloseSend()
	return p.pool.Put(p.conn)
}

type raftRPCTransportAppendFuture struct {
	start    time.Time
	request  *raft.AppendEntriesRequest
	response *raft.AppendEntriesResponse
}

func (f *raftRPCTransportAppendFuture) Error() error {
	return nil
}

func (f *raftRPCTransportAppendFuture) Start() time.Time {
	return f.start
}

func (f *raftRPCTransportAppendFuture) Request() *raft.AppendEntriesRequest {
	return f.request
}

func (f *raftRPCTransportAppendFuture) Response() *raft.AppendEntriesResponse {
	return f.response
}

func (t *RaftRPCTransport) AppendEntries(id raft.ServerID, target raft.ServerAddress, request *raft.AppendEntriesRequest, response *raft.AppendEntriesResponse) error {
	conn, err := t.pool.Get(context.Background(), string(target))
	if err != nil {
		return err
	}
	defer t.pool.Put(conn)

	client := NewRaftRPCClient(conn)
	stream, err := client.DoAppendEntries(context.Background())
	if err != nil {
		return err
	}
	defer stream.CloseSend()

	var entries []*AppendEntriesRequest_Entry
	for _, entry := range request.Entries {
		entries = append(entries, &AppendEntriesRequest_Entry{
			Index: entry.Index,
			Term:  entry.Term,
			Type:  AppendEntriesRequest_Entry_Type(entry.Type),
			Data:  entry.Data,
		})
	}
	err = stream.Send(&AppendEntriesRequest{
		Term:              request.Term,
		Leader:            request.Leader,
		PrevLogEntry:      request.PrevLogEntry,
		PrevLogTerm:       request.PrevLogTerm,
		Entries:           entries,
		LeaderCommitIndex: request.LeaderCommitIndex,
	})
	if err != nil {
		return err
	}

	rpcResponse, err := stream.Recv()
	if err != nil {
		return err
	}

	*response = raft.AppendEntriesResponse{
		Term:           rpcResponse.Term,
		LastLog:        rpcResponse.LastLog,
		Success:        rpcResponse.Success,
		NoRetryBackoff: rpcResponse.NoRetryBackoff,
	}

	return nil
}

func (t *RaftRPCTransport) RequestVote(id raft.ServerID, target raft.ServerAddress, request *raft.RequestVoteRequest, response *raft.RequestVoteResponse) error {
	conn, err := t.pool.Get(context.Background(), string(target))
	if err != nil {
		return err
	}
	defer t.pool.Put(conn)

	client := NewRaftRPCClient(conn)
	rpcResponse, err := client.DoRequestVote(context.Background(), &RequestVoteRequest{
		Term:         request.Term,
		Candidate:    request.Candidate,
		LastLogIndex: request.LastLogIndex,
		LastLogTerm:  request.LastLogTerm,
	})
	if err != nil {
		return err
	}

	*response = raft.RequestVoteResponse{
		Term:    rpcResponse.Term,
		Granted: rpcResponse.Granted,
	}

	return nil
}

func (t *RaftRPCTransport) InstallSnapshot(id raft.ServerID, target raft.ServerAddress, request *raft.InstallSnapshotRequest, response *raft.InstallSnapshotResponse, data io.Reader) (err error) {
	conn, err := t.pool.Get(context.Background(), string(target))
	if err != nil {
		return err
	}
	defer t.pool.Put(conn)

	client := NewRaftRPCClient(conn)
	stream, err := client.DoInstallSnapshot(context.Background())
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			stream.CloseSend()
		}
	}()

	err = stream.Send(&InstallSnapshotRequest{
		Body: &InstallSnapshotRequest_Request_{
			Request: &InstallSnapshotRequest_Request{
				Term:               request.Term,
				Leader:             request.Leader,
				LastLogIndex:       request.LastLogIndex,
				LastLogTerm:        request.LastLogTerm,
				Configuration:      request.Configuration,
				ConfigurationIndex: request.ConfigurationIndex,
				Size_:              request.Size,
			},
		},
	})
	if err != nil {
		return err
	}

	buf := make([]byte, installSnapshotBufferSize)
	for {
		buf = buf[:cap(buf)]
		n, err := data.Read(buf)
		eof := false
		if err == io.EOF {
			eof = true
		} else if err != nil {
			return err
		}
		err = stream.Send(&InstallSnapshotRequest{
			Body: &InstallSnapshotRequest_Data{
				Data: buf[:n],
			},
		})
		if err != nil {
			return err
		}
		if eof {
			break
		}
	}

	rpcResponse, err := stream.CloseAndRecv()
	if err != nil {
		return err
	}

	*response = raft.InstallSnapshotResponse{
		Term:    rpcResponse.Term,
		Success: rpcResponse.Success,
	}

	return nil
}

func (t *RaftRPCTransport) EncodePeer(id raft.ServerID, addr raft.ServerAddress) []byte {
	return []byte(string(id) + "@" + string(addr))
}

func (t *RaftRPCTransport) DecodePeer(peer []byte) raft.ServerAddress {
	index := bytes.IndexByte(peer, '@')
	return raft.ServerAddress(peer[index+1:])
}

func (t *RaftRPCTransport) SetHeartbeatHandler(handler func(rpc raft.RPC)) {
	t.mutex.Lock()
	t.heartbeatHandler = handler
	t.mutex.Unlock()
}

func (t *RaftRPCTransport) DoAppendEntries(stream RaftRPC_DoAppendEntriesServer) error {
	ctx := stream.Context()
	responseC := make(chan raft.RPCResponse, 1)

	for {
		request, err := stream.Recv()
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}

		var entries []*raft.Log

		for _, entry := range request.Entries {
			entries = append(entries, &raft.Log{
				Index: entry.Index,
				Term:  entry.Term,
				Type:  raft.LogType(entry.Type),
				Data:  entry.Data,
			})
		}

		call := raft.RPC{
			Command: &raft.AppendEntriesRequest{
				RPCHeader: raft.RPCHeader{
					ProtocolVersion: raft.ProtocolVersionMax,
				},
				Term:              request.Term,
				Leader:            request.Leader,
				PrevLogEntry:      request.PrevLogEntry,
				PrevLogTerm:       request.PrevLogTerm,
				Entries:           entries,
				LeaderCommitIndex: request.LeaderCommitIndex,
			},
			RespChan: responseC,
		}

		isHeartbeat := request.Term != 0 && request.Leader != nil &&
			request.PrevLogEntry == 0 && request.PrevLogTerm == 0 &&
			len(request.Entries) == 0 && request.LeaderCommitIndex == 0

		if isHeartbeat {
			t.mutex.RLock()
			handleHeartbeat := t.heartbeatHandler
			t.mutex.RUnlock()

			if handleHeartbeat != nil {
				handleHeartbeat(call)
				goto Response
			}
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case t.ch <- call:
		}

	Response:
		var responseOrError raft.RPCResponse
		select {
		case <-ctx.Done():
			return ctx.Err()
		case responseOrError = <-responseC:
		}

		if responseOrError.Error != nil {
			return responseOrError.Error
		}

		response := responseOrError.Response.(*raft.AppendEntriesResponse)

		err = stream.Send(&AppendEntriesResponse{
			Term:           response.Term,
			LastLog:        response.LastLog,
			Success:        response.Success,
			NoRetryBackoff: response.NoRetryBackoff,
		})
		if err != nil {
			return err
		}
	}

	return nil
}

func (t *RaftRPCTransport) DoRequestVote(ctx context.Context, request *RequestVoteRequest) (*RequestVoteResponse, error) {
	responseC := make(chan raft.RPCResponse, 1)

	t.ch <- raft.RPC{
		Command: &raft.RequestVoteRequest{
			RPCHeader: raft.RPCHeader{
				ProtocolVersion: raft.ProtocolVersionMax,
			},
			Term:         request.Term,
			Candidate:    request.Candidate,
			LastLogIndex: request.LastLogIndex,
			LastLogTerm:  request.LastLogTerm,
		},
		RespChan: responseC,
	}

	var responseOrError raft.RPCResponse
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case responseOrError = <-responseC:
	}

	if responseOrError.Error != nil {
		return nil, responseOrError.Error
	}

	response := responseOrError.Response.(*raft.RequestVoteResponse)

	return &RequestVoteResponse{
		Term:    response.Term,
		Granted: response.Granted,
	}, nil
}

func (t *RaftRPCTransport) DoInstallSnapshot(stream RaftRPC_DoInstallSnapshotServer) error {
	in, err := stream.Recv()
	if err != nil {
		return err
	}

	request := in.GetRequest()
	if request == nil {
		return errors.New("expected to receive request first")
	}

	pr, pw := io.Pipe()

	responseC := make(chan raft.RPCResponse, 1)

	t.ch <- raft.RPC{
		Command: &raft.InstallSnapshotRequest{
			RPCHeader: raft.RPCHeader{
				ProtocolVersion: raft.ProtocolVersionMax,
			},
			Term:               request.Term,
			Leader:             request.Leader,
			LastLogIndex:       request.LastLogIndex,
			LastLogTerm:        request.LastLogTerm,
			Configuration:      request.Configuration,
			ConfigurationIndex: request.ConfigurationIndex,
			Size:               request.Size_,
		},
		Reader:   pr,
		RespChan: responseC,
	}

	for {
		in, err := stream.Recv()
		if err == io.EOF {
			pw.Close()
			break
		} else if err != nil {
			pw.CloseWithError(err)
			return err
		}

		data := in.GetData()
		if data == nil {
			err = errors.New("expected to receive data")
			pw.CloseWithError(err)
			return err
		}

		_, err = pw.Write(data)
		if err != nil {
			return err
		}
	}

	var responseOrError raft.RPCResponse
	select {
	case <-stream.Context().Done():
		return context.Canceled
	case responseOrError = <-responseC:
	}

	if responseOrError.Error != nil {
		return responseOrError.Error
	}

	response := responseOrError.Response.(*raft.InstallSnapshotResponse)

	stream.SendAndClose(&InstallSnapshotResponse{
		Term:    response.Term,
		Success: response.Success,
	})

	return nil
}
