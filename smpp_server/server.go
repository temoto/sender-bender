package smpp_server

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"github.com/temoto/go-smpp/smpp/pdu"
	"github.com/temoto/senderbender/alive"
	"github.com/temoto/senderbender/junk"
	"io"
	"log"
	"net"
	"net/url"
	"strconv"
	"time"
)

// SMPPServer: accept loop, graceful stop
// You may override HandleConn, default is `conn.Run()`
type SMPPServer struct {
	HandleConn        SMPPConnHandler
	IdleTimeout       time.Duration
	ReceiveTimeout    time.Duration
	SendTimeout       time.Duration
	alive             *alive.Alive
	l                 net.Listener
	defaultPduHandler PduHandler
}

type SMPPConnHandler func(context.Context, *SMPPServer, *SMPPConn)

func NewSMPPServer(listenURL string, handleConn SMPPConnHandler, handlePdu PduHandler) (*SMPPServer, error) {
	var err error
	if handleConn == nil {
		handleConn = defaultHandleConn
	}
	s := &SMPPServer{
		HandleConn:        handleConn,
		IdleTimeout:       300 * time.Second,
		ReceiveTimeout:    21 * time.Second,
		SendTimeout:       21 * time.Second,
		alive:             alive.NewAlive(),
		defaultPduHandler: handlePdu,
	}
	var u *url.URL
	if u, err = url.Parse(listenURL); err != nil {
		return nil, err
	}
	if s.l, err = net.Listen(u.Scheme, u.Host); err != nil {
		return nil, err
	}
	log.Printf("smpp_server %s", s)
	return s, nil
}

func MustSMPPServer(listenURL string, handleConn SMPPConnHandler, handlePdu PduHandler) *SMPPServer {
	s, err := NewSMPPServer(listenURL, handleConn, handlePdu)
	if err != nil {
		log.Fatal(err)
	}
	return s
}

func defaultHandleConn(ctx context.Context, srv *SMPPServer, sc *SMPPConn) {
	sc.Run(ctx)
}

func (self *SMPPServer) runSMPPConn(ctx context.Context, c *SMPPConn) {
	go func(srva *alive.Alive) {
		<-srva.StopChan()
		c.alive.Stop()
	}(self.alive)
	self.HandleConn(ctx, self, c)
	c.alive.Wait()
	c.Close()
	self.alive.Done()
}

func (self *SMPPServer) Run(ctx context.Context) {
	for {
		conn, err := self.l.Accept()
		if err != nil {
			if !self.alive.IsRunning() {
				return
			}
			log.Printf("smpp_server %s Accept() error: %s", self, err.Error())
			self.Stop(err) // or not?
			// continue
		}
		self.alive.Add(1)
		smppSMPPConn := newSMPPConn(ctx, self, conn)
		go self.runSMPPConn(ctx, smppSMPPConn)
	}
}

func (self *SMPPServer) Stop(err error) {
	defer self.l.Close()
	defer self.alive.Stop()
	status := "OK"
	if err != nil {
		status = err.Error()
	}
	log.Printf("smpp_server %s Stop() status: %s", self, status)
}

func (self *SMPPServer) Wait() {
	self.alive.Wait()
}

func (self *SMPPServer) String() string {
	return fmt.Sprintf("%s listen=%s", self.alive, self.l.Addr())
}

// per connection logic

type SMPPConn struct {
	HandlePdu PduHandler
	Bound     int
	alive     *alive.Alive
	c         net.Conn
	chin      chan pdu.Body
	chout     chan pdu.Body
	idle      *time.Timer
	srv       *SMPPServer
}

type PduHandler func(context.Context, *SMPPConn, pdu.Body)

func newSMPPConn(ctx context.Context, srv *SMPPServer, conn net.Conn) *SMPPConn {
	c := &SMPPConn{
		HandlePdu: srv.defaultPduHandler,
		c:         conn,
		srv:       srv,
		alive:     alive.NewAlive(),
		chin:      make(chan pdu.Body, 2),
		chout:     make(chan pdu.Body, 1),
		idle:      time.NewTimer(srv.IdleTimeout),
	}
	c.idle.Stop()
	return c
}

func (self *SMPPConn) reader() {
	defer self.alive.Stop()
	r := bufio.NewReader(self.c)
	for {
		if !self.alive.IsRunning() {
			if tcp, ok := self.c.(*net.TCPConn); ok {
				tcp.CloseRead()
			}
			return
		}
		if err := self.c.SetReadDeadline(time.Now().Add(self.srv.ReceiveTimeout)); err != nil {
			log.Printf("smpp_server %s reader set deadline error: %s", self, err.Error())
			return
		}
		p, err := pdu.Decode(r)
		junk.ResetTimer(self.idle, self.srv.IdleTimeout)
		if err := self.c.SetReadDeadline(time.Time{}); err != nil {
			log.Printf("smpp_server %s reader reset deadline error: %s", self, err.Error())
			return
		}
		if err == nil {
			self.chin <- p
		} else if err == io.EOF {
			// debug only
			// log.Printf("smpp_server %s client closed connection", self)
			return
		} else {
			log.Printf("smpp_server %s network read error: %s", self, err.Error())
			return
		}
		if self.alive.IsFinished() {
			return
		}
	}
}

func (self *SMPPConn) writer() {
	defer self.alive.Stop()
	b := bytes.NewBuffer(nil)
	finish := self.alive.WaitChan()
	for {
		if err := self.c.SetWriteDeadline(time.Time{}); err != nil {
			log.Printf("smpp_server %s writer reset deadline error: %s", self, err.Error())
			return
		}
		select {
		case <-finish:
			return
		case p := <-self.chout:
			b.Reset()
			if err := self.c.SetWriteDeadline(time.Now().Add(self.srv.SendTimeout)); err != nil {
				log.Printf("smpp_server %s writer reset deadline error: %s", self, err.Error())
				return
			}
			if err := p.SerializeTo(b); err != nil {
				log.Printf("smpp_server %s serialize error: %s", self, err.Error())
				return
			}
			if _, err := b.WriteTo(self.c); err != nil {
				log.Printf("smpp_server %s network write error: %s", self, err.Error())
				return
			}
			junk.ResetTimer(self.idle, self.srv.IdleTimeout)
		}
	}
}

func (self *SMPPConn) Run(ctx context.Context) {
	defer self.alive.Stop()
	go self.reader()
	go self.writer()
	stopch := self.alive.StopChan()
	for {
		select {
		case <-stopch:
			return
		case p := <-self.chin:
			ctx = context.WithValue(ctx, "request-id", strconv.FormatUint(uint64(p.Header().Seq), 10))
			self.HandlePdu(ctx, self, p)
		case <-self.idle.C:
			return
		}
	}
}

const (
	Unbound = 1 << iota
	BoundTx
	BoundRx
)

var pduRespMap = map[pdu.ID]func() pdu.Body{
	pdu.BindReceiverID:    pdu.NewBindReceiverResp,
	pdu.BindTransceiverID: pdu.NewBindTransceiverResp,
	pdu.BindTransmitterID: pdu.NewBindTransmitterResp,
	pdu.DeliverSMID:       pdu.NewDeliverSMResp,
	pdu.EnquireLinkID:     pdu.NewEnquireLinkResp,
	pdu.QuerySMID:         pdu.NewQuerySMResp,
	pdu.SubmitMultiID:     pdu.NewSubmitMultiResp,
	pdu.SubmitSMID:        pdu.NewSubmitSMResp,
	pdu.UnbindID:          pdu.NewUnbindResp,
}

func (self *SMPPConn) BuildResp(req pdu.Body) pdu.Body {
	f, ok := pduRespMap[req.Header().ID]
	if !ok {
		return nil
	}
	resp := f()
	resp.Header().Seq = req.Header().Seq
	return resp
}

func (self *SMPPConn) Send(p pdu.Body) {
	self.chout <- p
}

func (self *SMPPConn) IsBoundRx() bool { return self.Bound&BoundRx != 0 }
func (self *SMPPConn) IsBoundTx() bool { return self.Bound&BoundTx != 0 }

func (self *SMPPConn) Close() {
	self.alive.Stop()
	self.idle.Stop()
	if err := self.c.Close(); err != nil {
		log.Printf("smpp_server %s connection close error: %s", self, err.Error())
	}
}

func (self *SMPPConn) StopServer() { self.srv.Stop(nil) }

func (self *SMPPConn) String() string {
	return fmt.Sprintf("remote_addr=%s", self.c.RemoteAddr())
}
