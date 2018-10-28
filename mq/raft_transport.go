package mq

import (
	"bytes"
	"context"
	"io"
	"net"
	"strconv"
	"time"

	"github.com/hashicorp/raft"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
)

const (
	installSnapshotBufferSize = 65536
)

type RaftRPCTransport struct {
	advertiseIP   net.IP
	advertisePort int
	pool          *ClientConnPool
	ch            chan raft.RPC
}

func NewRaftRPCTransport(advertiseIP net.IP, advertisePort int, pool *ClientConnPool) *RaftRPCTransport {
	return &RaftRPCTransport{
		advertiseIP:   advertiseIP,
		advertisePort: advertisePort,
		pool:          pool,
		ch:            make(chan raft.RPC),
	}
}

func (t *RaftRPCTransport) Consumer() <-chan raft.RPC {
	return t.ch
}

func (t *RaftRPCTransport) LocalAddr() raft.ServerAddress {
	return raft.ServerAddress(t.advertiseIP.String() + ":" + strconv.Itoa(t.advertisePort))
}

func (t *RaftRPCTransport) AppendEntriesPipeline(id raft.ServerID, target raft.ServerAddress) (raft.AppendPipeline, error) {
	conn, err := t.pool.Get(context.Background(), string(target))
	if err != nil {
		return nil, err
	}

	return &raftRPCTransportAppendPipeline{
		pool:   t.pool,
		conn:   conn,
		client: NewRaftRPCClient(conn),
		ch:     make(chan raft.AppendFuture),
	}, nil
}

type raftRPCTransportAppendPipeline struct {
	pool   *ClientConnPool
	conn   *grpc.ClientConn
	client RaftRPCClient
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
	rpcResponse, err := p.client.DoAppendEntries(context.Background(), &AppendEntriesRequest{
		Term:              request.Term,
		Leader:            request.Leader,
		PrevLogEntry:      request.PrevLogEntry,
		PrevLogTerm:       request.PrevLogTerm,
		Entries:           entries,
		LeaderCommitIndex: request.LeaderCommitIndex,
	})

	var future raft.AppendFuture
	if err != nil {
		future = &raftRPCTransportAppendFuture{
			start:   start,
			request: request,
			err:     err,
		}
	} else {
		*response = raft.AppendEntriesResponse{
			Term:           rpcResponse.Term,
			LastLog:        rpcResponse.LastLog,
			Success:        rpcResponse.Success,
			NoRetryBackoff: rpcResponse.NoRetryBackoff,
		}

		future = &raftRPCTransportAppendFuture{
			start:    time.Now(),
			request:  request,
			response: response,
		}
	}

	p.ch <- future

	return future, nil
}

func (p *raftRPCTransportAppendPipeline) Consumer() <-chan raft.AppendFuture {
	return p.ch
}

func (p *raftRPCTransportAppendPipeline) Close() error {
	return p.pool.Put(p.conn)
}

type raftRPCTransportAppendFuture struct {
	start    time.Time
	request  *raft.AppendEntriesRequest
	err      error
	response *raft.AppendEntriesResponse
}

func (f *raftRPCTransportAppendFuture) Error() error {
	return f.err
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
	var entries []*AppendEntriesRequest_Entry
	for _, entry := range request.Entries {
		entries = append(entries, &AppendEntriesRequest_Entry{
			Index: entry.Index,
			Term:  entry.Term,
			Type:  AppendEntriesRequest_Entry_Type(entry.Type),
			Data:  entry.Data,
		})
	}
	rpcResponse, err := client.DoAppendEntries(context.Background(), &AppendEntriesRequest{
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

	err = stream.Send(&InstallSnapshot{
		Body: &InstallSnapshot_Request_{
			Request: &InstallSnapshot_Request{
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
		if err != nil {
			return err
		}
		err = stream.Send(&InstallSnapshot{
			Body: &InstallSnapshot_Data{
				Data: buf[:n],
			},
		})
		if err != nil {
			return err
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

func (t *RaftRPCTransport) SetHeartbeatHandler(cb func(rpc raft.RPC)) {
	// TODO
}

func (t *RaftRPCTransport) DoAppendEntries(ctx context.Context, request *AppendEntriesRequest) (*AppendEntriesResponse, error) {
	responseC := make(chan raft.RPCResponse, 1)

	var entries []*raft.Log

	for _, entry := range request.Entries {
		entries = append(entries, &raft.Log{
			Index: entry.Index,
			Term:  entry.Term,
			Type:  raft.LogType(entry.Type),
			Data:  entry.Data,
		})
	}

	t.ch <- raft.RPC{
		Command: &raft.AppendEntriesRequest{
			Term:              request.Term,
			Leader:            request.Leader,
			PrevLogEntry:      request.PrevLogEntry,
			PrevLogTerm:       request.PrevLogTerm,
			Entries:           entries,
			LeaderCommitIndex: request.LeaderCommitIndex,
		},
		RespChan: responseC,
	}

	var responseOrError raft.RPCResponse
	select {
	case <-ctx.Done():
		return nil, context.Canceled
	case responseOrError = <-responseC:
	}

	if responseOrError.Error != nil {
		return nil, responseOrError.Error
	}

	response := responseOrError.Response.(*raft.AppendEntriesResponse)

	return &AppendEntriesResponse{
		Term:           response.Term,
		LastLog:        response.LastLog,
		Success:        response.Success,
		NoRetryBackoff: response.NoRetryBackoff,
	}, nil
}

func (t *RaftRPCTransport) DoRequestVote(ctx context.Context, request *RequestVoteRequest) (*RequestVoteResponse, error) {
	responseC := make(chan raft.RPCResponse, 1)

	t.ch <- raft.RPC{
		Command: &raft.RequestVoteRequest{
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
		return nil, context.Canceled
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
