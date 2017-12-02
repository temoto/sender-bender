package main

import (
	"context"
	"flag"
	"github.com/coreos/go-systemd/daemon"
	"github.com/fiorix/go-smpp/smpp/pdu"
	"github.com/fiorix/go-smpp/smpp/pdu/pdufield"
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

	duraqueue := new(duraqueue.DuraQueue)
	sender := new(Sender)
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
	duraqueue.Init(ctx, *flagState, TaskHandle)
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
		if !c.IsBoundTx() {
			log.Printf("smpp_server %s submit_sm without bind_tx ... watevah", c)
		}
		msg := bendertalk.MessageFromPdu(p, true)
		log.Printf("smpp_server %s submit_sm seq=%d from=%s to=%s text='%s'",
			c, p.Header().Seq, msg.From, msg.To, msg.Text)
		msg.ScheduleUnix = 1
		contextGetDuraQueue(ctx).PushTalkSMSMessage(ctx, msg)

		resp := c.BuildResp(p)
		resp.Header().Status = 0
		resp.Fields().Set(pdufield.MessageID, "randomshit")
		c.Send(resp)

		// debug stop after first message
		time.AfterFunc(1*time.Second, c.StopServer)
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

	msg, err := t.ToTalk()
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
	case bendertalk.SMPPMessageState_Attempt:
		response, err := upstream.Submit(msg.ToFiorix())
		if err != nil {
			log.Printf("TaskHandle Submit failed msg=%s error=%s will retry", msg, err)
			t.Scheduled = now.Add(5 * time.Minute)
			t.Store(ctx)
			return
		}
		msg.Id = response.RespID()
		msg.State = bendertalk.SMPPMessageState_Accepted
		t.Scheduled = now.Add(5 * time.Minute)
		t.Modified = now
		t.Data, err = msg.Marshal()
		if err != nil {
			log.Printf("BUG TaskHandle msg.Marshal() msg=%s error=%s", msg, err)
		}
		t.Store(ctx)
		return
		// TODO handle long messages, smpp.SubmitLong makes UDH cuts
	case bendertalk.SMPPMessageState_Accepted:
		log.Printf("TODO TaskHandle invalid state=%d msg=%s", msg.State, msg)
	case bendertalk.SMPPMessageState_Enroute:
		log.Printf("TODO TaskHandle invalid state=%d msg=%s", msg.State, msg)
	case bendertalk.SMPPMessageState_Expired:
		log.Printf("TODO TaskHandle invalid state=%d msg=%s", msg.State, msg)
	case bendertalk.SMPPMessageState_Rejected, bendertalk.SMPPMessageState_Delivered, bendertalk.SMPPMessageState_Deleted,
		bendertalk.SMPPMessageState_Unknown, bendertalk.SMPPMessageState_Undeliverable, bendertalk.SMPPMessageState_Skipped:
		t.Archive(ctx)
		return
	default:
		log.Printf("BUG TaskHandle invalid state=%d msg=%s", msg.State, msg)
	}

	return
}
