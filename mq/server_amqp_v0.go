package mq

import (
	"context"
	"net"
	"time"

	"eventter.io/mq/amqp/v0"
	"github.com/pkg/errors"
)

func (s *Server) HandleAMQPv0(conn net.Conn, sc *v0.ServerConn) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	frames := make(chan v0.Frame, 64)
	errc := make(chan error, 2)

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				if err := conn.SetReadDeadline(time.Now().Add(2 * sc.Heartbeat)); err != nil {
					errc <- errors.Wrap(err, "read deadline failed")
					return
				}
				frame, err := sc.Transport.Receive()
				if err != nil {
					errc <- errors.Wrap(err, "receive failed")
					return
				}
				frames <- frame
			}
		}
	}()

	channels := make(map[uint16]*serverAMQPv0Channel)

	for {
		select {
		case <-s.closeC:
			return forceCloseAMQPv0Connection(conn, sc, v0.ConnectionForced, "shutdown")
		case frame := <-frames:
			switch frame := frame.(type) {
			case v0.MethodFrame:
				meta := frame.GetFrameMeta()
				if meta.Channel == 0 {
					switch frame := frame.(type) {
					case *v0.ConnectionClose:
						if frame.ReplyCode == 0 || frame.ReplyCode == v0.ReplySuccess {
							return sendAMQPv0(conn, sc, &v0.ConnectionCloseOk{})
						} else {
							return errors.Errorf("received connection error %q (%d)", frame.ReplyText, frame.ReplyCode)
						}
					case *v0.ConnectionCloseOk:
						return nil
					default:
						return forceCloseAMQPv0Connection(conn, sc, v0.SyntaxError, "non-close frame")
					}
				} else {
					switch frame.(type) {
					case *v0.ChannelOpen:
						if _, ok := channels[meta.Channel]; ok {
							return forceCloseAMQPv0Connection(conn, sc, v0.ChannelError, "channel already open")
						}
						channels[meta.Channel] = &serverAMQPv0Channel{
							// TODO
						}
						err := sendAMQPv0(conn, sc, &v0.ChannelOpenOk{FrameMeta: v0.FrameMeta{Channel: meta.Channel}})
						if err != nil {
							return forceCloseAMQPv0Connection(conn, sc, v0.InternalError, err.Error())
						}
					default:
						ch, ok := channels[meta.Channel]
						if !ok {
							return forceCloseAMQPv0Connection(conn, sc, v0.ChannelError, "channel not open")
						}
						err := s.handleAMQPv0ChannelMethod(conn, sc, ch, frame)
						if err != nil {
							if connClose, ok := err.(*v0.ConnectionClose); ok {
								return sendAMQPv0(conn, sc, connClose)
							} else if chanClose, ok := err.(*v0.ChannelClose); ok {
								err = sendAMQPv0(conn, sc, chanClose)
								if err != nil {
									return forceCloseAMQPv0Connection(conn, sc, v0.InternalError, err.Error())
								}
							} else {
								return forceCloseAMQPv0Connection(conn, sc, v0.InternalError, err.Error())
							}
						}
					}
				}
			case *v0.ContentHeaderFrame:
				if frame.FrameMeta.Channel == 0 {
					return forceCloseAMQPv0Connection(conn, sc, v0.SyntaxError, "content header frame on zero channel")
				}
				ch, ok := channels[frame.FrameMeta.Channel]
				if !ok {
					return forceCloseAMQPv0Connection(conn, sc, v0.ChannelError, "channel not open")
				}
				err := s.handleAMQPv0ChannelContentHeader(conn, sc, ch, frame)
				if err != nil {
					if connClose, ok := err.(*v0.ConnectionClose); ok {
						return sendAMQPv0(conn, sc, connClose)
					} else if chanClose, ok := err.(*v0.ChannelClose); ok {
						err = sendAMQPv0(conn, sc, chanClose)
						if err != nil {
							return forceCloseAMQPv0Connection(conn, sc, v0.InternalError, err.Error())
						}
					} else {
						return forceCloseAMQPv0Connection(conn, sc, v0.InternalError, err.Error())
					}
				}
			case *v0.ContentBodyFrame:
				if frame.FrameMeta.Channel == 0 {
					return forceCloseAMQPv0Connection(conn, sc, v0.SyntaxError, "content body frame on zero channel")
				}
				ch, ok := channels[frame.FrameMeta.Channel]
				if !ok {
					return forceCloseAMQPv0Connection(conn, sc, v0.ChannelError, "channel not open")
				}
				err := s.handleAMQPv0ChannelContentBody(conn, sc, ch, frame)
				if err != nil {
					if connClose, ok := err.(*v0.ConnectionClose); ok {
						return sendAMQPv0(conn, sc, connClose)
					} else if chanClose, ok := err.(*v0.ChannelClose); ok {
						err = sendAMQPv0(conn, sc, chanClose)
						if err != nil {
							return forceCloseAMQPv0Connection(conn, sc, v0.InternalError, err.Error())
						}
					} else {
						return forceCloseAMQPv0Connection(conn, sc, v0.InternalError, err.Error())
					}
				}
			case *v0.HeartbeatFrame:
				if frame.FrameMeta.Channel != 0 {
					return forceCloseAMQPv0Connection(conn, sc, v0.SyntaxError, "heartbeat frame on non-zero channel")
				}
			default:
				return errors.Errorf("unhandled frame of type %T", frame)
			}
		case err := <-errc:
			if err == v0.ErrMalformedFrame {
				err = forceCloseAMQPv0Connection(conn, sc, v0.FrameError, "malformed frame")
			}
			return err
		}
	}
}

type serverAMQPv0Channel struct {
}

func (s *Server) handleAMQPv0ChannelMethod(conn net.Conn, sc *v0.ServerConn, ch *serverAMQPv0Channel, frame v0.MethodFrame) error {
	panic("implement me")
}

func (s *Server) handleAMQPv0ChannelContentHeader(conn net.Conn, sc *v0.ServerConn, ch *serverAMQPv0Channel, frame *v0.ContentHeaderFrame) error {
	panic("implement me")
}

func (s *Server) handleAMQPv0ChannelContentBody(conn net.Conn, sc *v0.ServerConn, ch *serverAMQPv0Channel, frame *v0.ContentBodyFrame) error {
	panic("implement me")
}

func sendAMQPv0(conn net.Conn, sc *v0.ServerConn, frame v0.Frame) error {
	err := conn.SetWriteDeadline(time.Now().Add(sc.Heartbeat))
	if err != nil {
		return errors.Wrap(err, "set write deadline failed")
	}
	err = sc.Transport.Send(frame)
	if err != nil {
		return errors.Wrap(err, "send failed")
	}
	return nil
}

func forceCloseAMQPv0Connection(conn net.Conn, sc *v0.ServerConn, replyCode uint16, replyText string) error {
	err := sendAMQPv0(conn, sc, &v0.ConnectionClose{
		ReplyCode: replyCode,
		ReplyText: replyText,
	})
	if err != nil {
		return errors.Wrap(err, "force close failed")
	}
	return nil
}
