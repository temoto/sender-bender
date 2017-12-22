package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/coreos/go-systemd/daemon"
	"github.com/temoto/go-smpp/smpp/pdu"
	"github.com/temoto/go-smpp/smpp/pdu/pdufield"
	"github.com/temoto/senderbender/duraqueue"
	"github.com/temoto/senderbender/junk"
	"github.com/temoto/senderbender/smpp_server"
	"github.com/temoto/senderbender/talk"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	var (
		flagListenSMPP   = flag.String("listen-smpp", "tcp://:2775", "Listen SMPP on tcp://host:port")
		flagUpstreamSMPP = flag.String("upstream-smpp", "", "Connect to real SMPP 'tcp://user:pass@host:port?options url2...'")
		flagState        = flag.String("state", "/tmp/senderbender.sqlite", "Sqlite db url")
		flagDebug        = flag.Bool("debug", false, "Enable debug logging")
	)
	flag.Parse()

	// systemd service
	sdnotify("READY=0\nSTATUS=init\n")
	if wdTime, err := daemon.SdWatchdogEnabled(true); err != nil {
		log.Fatal(err)
	} else if wdTime != 0 {
		go func() {
			for range time.Tick(wdTime) {
				sdnotify("WATCHDOG=1\n")
			}
		}()
	}

	duraqueue := &duraqueue.DuraQueue{Handle: TaskHandle}
	sender := &Sender{}
	ctx := context.Background()
	ctx = junk.ContextSetMap(ctx, map[string]interface{}{
		"log-debug":            flagDebug,
		"db-timeout-fast":      3 * time.Second,
		"db-timeout-max":       11 * time.Second,
		"db-pull-interval":     37 * time.Second,
		"queue-size":           13,
		"duraqueue-pickup-age": 23 * time.Minute,
		"duraqueue":            duraqueue,
		"sender":               sender,
	})
	sender.Handle = func(p pdu.Body) { SenderPduHandle(ctx, p) }
	duraqueue.Init(ctx, *flagState)
	sender.Init(ctx, *flagUpstreamSMPP)
	smppServer := smpp_server.MustSMPPServer(*flagListenSMPP, nil, SMPPServerHandle)
	stopAll := func() {
		duraqueue.Stop()
		sender.Stop()
		smppServer.Stop(nil)
	}
	waitAll := func() {
		duraqueue.Wait()
		sender.Wait()
		smppServer.Wait()
	}

	sigShutdownChan := make(chan os.Signal, 1)
	signal.Notify(sigShutdownChan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	go func(ch <-chan os.Signal, f func()) {
		<-ch
		log.Printf("graceful stop")
		sdnotify("READY=0\nSTATUS=stopping\n")
		f()
	}(sigShutdownChan, stopAll)

	sdnotify("READY=1\nSTATUS=work\n")
	go duraqueue.Run(ctx)
	go smppServer.Run(ctx)

	waitAll()
}

func SMPPServerHandle(ctx context.Context, c *smpp_server.SMPPConn, p pdu.Body) {
	switch p.Header().ID {
	case pdu.BindReceiverID:
		c.Send(c.BuildResp(p))
		c.Bound = smpp_server.BoundRx
	case pdu.BindTransmitterID:
		c.Send(c.BuildResp(p))
		c.Bound = smpp_server.BoundTx
	case pdu.BindTransceiverID:
		c.Send(c.BuildResp(p))
		c.Bound = smpp_server.BoundRx | smpp_server.BoundTx
	case pdu.EnquireLinkID:
		c.Send(c.BuildResp(p))
	case pdu.UnbindID:
		c.Bound = smpp_server.Unbound

	case pdu.SubmitSMID:
		msg := bendertalk.MessageFromPdu(p, true)
		log.Printf("smpp_server %s submit_sm seq=%d from=%s to=%s text='%s'",
			c, p.Header().Seq, msg.From, msg.To, msg.Text)
		msg.ScheduleUnix = 1
		dqId, _ := contextGetDuraQueue(ctx).PushSMSMessage(ctx, msg)

		resp := c.BuildResp(p)
		resp.Header().Status = 0
		resp.Fields().Set(pdufield.MessageID, dqId)
		c.Send(resp)
		break

	case pdu.QuerySMID:
		queryMessageId := p.Fields()[pdufield.MessageID].String()
		querySourceAddr := p.Fields()[pdufield.SourceAddr].String()
		resp := c.BuildResp(p)
		resp.Fields().Set(pdufield.MessageID, queryMessageId)

		task, err := contextGetDuraQueue(ctx).QueryId(ctx, queryMessageId)
		logPrefix := fmt.Sprintf("smpp_server %s query_sm seq=%d req.message_id=%s req.source_addr=%s",
			c, p.Header().Seq, queryMessageId, querySourceAddr)
		if err != nil {
			log.Printf("%s QueryId error=%s", logPrefix, err)
			resp.Header().Status = 0x08 // ESME_RSYSERR
			c.Send(resp)
			break
		}

		if task == nil {
			resp.Header().Status = 0x67 // ESME_RQUERYFAIL
		} else if task.MsgFrom != querySourceAddr {
			log.Printf("%s task.MsgFrom=%s does not match req.srcaddr")
			resp.Header().Status = 0x0a // ESME_RINVSRCADR
		} else {
			resp.Header().Status = 0 // ESME_ROK
			if msg, err := task.ToSMSMessage(); err != nil {
				resp.Fields().Set(pdufield.MessageState, msg.State)
				resp.Fields().Set(pdufield.ErrorCode, msg.NetworkErrorCode)
			} else {
				state := bendertalk.SMPPMessageStateEnroute
				if task.Scheduled.IsZero() {
					resp.Fields().Set(pdufield.FinalDate, bendertalk.TimeToSMPP(task.Modified))
				}
				if st, ok := bendertalk.SMPPMessageState_value["SMPPMessageState"+task.Status]; ok {
					state = bendertalk.SMPPMessageState(st)
				}
				resp.Fields().Set(pdufield.MessageState, state)
			}
		}
		c.Send(resp)
		break

	default:
		log.Printf("smpp_server %s unknown pdu id %s (%08x) data %#v", c, p.Header().ID, int(p.Header().ID), p)
	}
}

func TaskHandle(ctx context.Context, t *duraqueue.Task) {
	sender := contextGetSender(ctx)

	now := time.Now().UTC().Truncate(time.Second)
	// t.Scheduled = now.Add(10 * time.Minute)
	t.Locked = time.Time{}
	t.Worker = ""

	msg, err := t.ToSMSMessage()
	if err != nil {
		log.Printf("BUG TaskHandle Task.ToTalk() t=%s error=%s will leave in queue with schedule=NULL need manual review", t, err)
		t.Scheduled = time.Time{}
		t.Store(ctx)
		return
	}
	log.Printf("debug TaskHandle SMSMessage id=%s from=%s to=%s text='%s'", msg.Id, msg.From, msg.To, msg.Text)

	upstream := sender.TxFor(msg)
	if upstream == nil {
		log.Printf("TaskHandle no upstream for message=%s will leave in queue with schedule=NULL need manual review", msg)
		t.Scheduled = time.Time{}
		t.Store(ctx)
		return
	}
	switch msg.State {
	case bendertalk.SMPPMessageStateAttempt:
		response, err := upstream.Submit(msg.ToFiorix())
		if err != nil {
			log.Printf("TaskHandle Submit failed msg=%s error=%s will retry", msg, err)
			t.Scheduled = now.Add(5 * time.Minute)
			t.Store(ctx)
			return
		}
		msg.Id = response.RespID()
		msg.State = bendertalk.SMPPMessageStateEnroute
		t.Scheduled = now.Add(5 * time.Minute)
		t.Modified = now
		t.Data, err = msg.Marshal()
		if err != nil {
			log.Printf("BUG TaskHandle msg.Marshal() msg=%s error=%s", msg, err)
		}
		t.Store(ctx)
		return
		// TODO handle long messages, smpp.SubmitLong makes UDH cuts
	case bendertalk.SMPPMessageStateAccepted:
		log.Printf("TODO TaskHandle invalid state=%d msg=%s", msg.State, msg)
	case bendertalk.SMPPMessageStateEnroute:
		log.Printf("TODO TaskHandle invalid state=%d msg=%s", msg.State, msg)
	case bendertalk.SMPPMessageStateExpired:
		log.Printf("TODO TaskHandle invalid state=%d msg=%s", msg.State, msg)
	case bendertalk.SMPPMessageStateRejected, bendertalk.SMPPMessageStateDelivered, bendertalk.SMPPMessageStateDeleted,
		bendertalk.SMPPMessageStateUnknown, bendertalk.SMPPMessageStateUndeliverable, bendertalk.SMPPMessageStateSkipped:
		t.Archive(ctx)
		return
	default:
		log.Printf("BUG TaskHandle invalid state=%d msg=%s", msg.State, msg)
	}

	return
}

func SenderPduHandle(ctx context.Context, p pdu.Body) {
	now := time.Now().UTC()

	switch p.Header().ID {
	case pdu.DeliverSMID:
		// esmClass := p.Fields()[pdufield.ESMClass].Raw().(uint8)
		messageState := p.TLVFields()[pdufield.MessageStateOption].Bytes()[0]
		networkErrorCode := p.TLVFields()[pdufield.NetworkErrorCode].Bytes()[0]
		receiptedMessageId := string(p.TLVFields()[pdufield.ReceiptedMessageID].Bytes())
		task, err := contextGetDuraQueue(ctx).QueryId(ctx, receiptedMessageId)
		requestSmpp := bendertalk.MessageFromPdu(p, true)
		logPrefix := fmt.Sprintf("sender deliver_sm seq=%d msg=%s pdu=%#v", p.Header().Seq, requestSmpp, p)
		if err != nil {
			log.Printf("%s QueryId error=%s", logPrefix, err)
			// resp.Header().Status = 0x64 // ESME_RX_T_APPN
			// FIXME: can't send reply because go-smpp will send autoreply. I'm afraid
			// some SMSC will go crazy seeing two DELIVER_SM_RESP in row with different content.
			return
		}
		if task == nil {
			log.Printf("%s SMSC sent deliver_sm packet for unknown message id=%s", logPrefix, receiptedMessageId)
			// resp.Header().Status = 0x66 // ESME_RX_R_APPN
			// FIXME: can't send reply because go-smpp will send autoreply. I'm afraid
			// some SMSC will go crazy seeing two DELIVER_SM_RESP in row with different content.
			return
		}

		task.Locked = time.Time{} // should be null anyway, but let's make sure twice
		msg, err := task.ToSMSMessage()
		if err != nil {
			log.Printf("%s Task.ToSMSMessage() error=%s", logPrefix, err)
			// resp.Header().Status = 0x64 // ESME_RX_T_APPN
			// FIXME: can't send reply because go-smpp will send autoreply. I'm afraid
			// some SMSC will go crazy seeing two DELIVER_SM_RESP in row with different content.
			return
		}
		msg.NetworkErrorCode = uint32(networkErrorCode)
		msg.State = bendertalk.SMPPMessageState(messageState)
		newStatus := duraqueue.TalkStateToStatus(msg.State)
		switch msg.State {
		case bendertalk.SMPPMessageStateAccepted, bendertalk.SMPPMessageStateEnroute:
			// So SMSC says message is in-flight? Let's schedule manual check, just in case.
			task.Scheduled = now.Add(30 * time.Minute)
			task.Status = newStatus
			task.Modified = now
			task.Store(ctx)
			return

		case bendertalk.SMPPMessageStateExpired:
			// Validity period expired, retry sending.
			if now.Sub(task.Created) > 14*24*time.Hour {
				// give up
				task.Scheduled = time.Time{}
				task.Status = newStatus
				task.Modified = now
				task.Archive(ctx)
				// TODO: send reply, but for now go-smpp does it for us
				return
			}
			task.Scheduled = now.Add(5 * time.Minute)
			task.Status = newStatus
			task.Modified = now
			task.Store(ctx)
			return

		case bendertalk.SMPPMessageStateRejected, bendertalk.SMPPMessageStateDelivered, bendertalk.SMPPMessageStateDeleted,
			bendertalk.SMPPMessageStateUnknown, bendertalk.SMPPMessageStateUndeliverable, bendertalk.SMPPMessageStateSkipped:
			task.Scheduled = time.Time{}
			task.Status = newStatus
			task.Modified = now
			task.Archive(ctx)
			return

			log.Printf("BUG %s SMSC sent deliver_sm packet with unknown message_state=%d", logPrefix, messageState)
		}
	}
}

func sdnotify(s string) {
	if _, err := daemon.SdNotify(false, s); err != nil {
		log.Fatal(err)
	}
}

func contextGetDuraQueue(ctx context.Context) *duraqueue.DuraQueue {
	a := ctx.Value("duraqueue")
	if a == nil {
		log.Printf("context[duraqueue] not found")
		return nil
	}
	if b, ok := a.(*duraqueue.DuraQueue); ok {
		return b
	}
	log.Fatalf("context[duraqueue] invalid value %#v", a)
	return nil
}
