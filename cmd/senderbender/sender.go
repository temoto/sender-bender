package main

import (
	"context"
	"github.com/fiorix/go-smpp/smpp"
	"github.com/temoto/senderbender/alive"
	"github.com/temoto/senderbender/talk"
	"golang.org/x/time/rate"
	"log"
	"net/url"
	"strconv"
	"strings"
	"sync"
)

type Sender struct {
	Handle smpp.HandlerFunc
	alive  *alive.Alive
	lk     sync.RWMutex
	trxs   map[string]*smpp.Transceiver
}

func queryGetInt(query url.Values, key string) (int, bool) {
	s := query.Get(key)
	if s == "" {
		return 0, false
	}
	i, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		log.Fatal(err)
	}
	return int(i), true
}

func (self *Sender) Init(ctx context.Context, s string) {
	self.lk.Lock()
	defer self.lk.Unlock()
	self.alive = alive.NewAlive()
	self.trxs = make(map[string]*smpp.Transceiver)
	for _, urlString := range strings.Split(s, " ") {
		if u, err := url.Parse(urlString); err != nil {
			log.Printf("url '%s' error %s", urlString, err)
		} else {
			trx := &smpp.Transceiver{Addr: u.Host}
			if u.User != nil {
				trx.User = u.User.Username()
				trx.Passwd, _ = u.User.Password()
			}
			if r, ok := queryGetInt(u.Query(), "rate_limit"); ok {
				trx.RateLimiter = rate.NewLimiter(rate.Limit(r), 1 /*burst*/)
			}
			trx.Bind()
			self.trxs[urlString] = trx
			self.alive.Add(1)
		}
	}
}

func (self *Sender) Stop() {
	for _, t := range self.trxs {
		t.Close()
		self.alive.Done()
	}
}

func (self *Sender) Wait() {
	for _, t := range self.trxs {
		t.Close()
	}
}

func (self *Sender) TxFor(msg *bendertalk.SMSMessage) *smpp.Transceiver {
	self.lk.RLock()
	defer self.lk.RUnlock()

	// TODO
	for _, trx := range self.trxs {
		// "elegant" way to get random value from map
		return trx
	}

	return nil
}

func contextGetSender(ctx context.Context) *Sender {
	a := ctx.Value("sender")
	if a == nil {
		log.Printf("context[sender] not found")
		return nil
	}
	if b, ok := a.(*Sender); ok {
		return b
	}
	log.Fatalf("context[sender] invalid value %#v", a)
	return nil
}
