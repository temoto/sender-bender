package duraqueue

import (
	"context"
	"database/sql"
	"fmt"
	_ "github.com/temoto/go-sqlite3" // happy golint
	"github.com/temoto/senderbender/alive"
	"github.com/temoto/senderbender/junk"
	"github.com/temoto/senderbender/talk"
	"log"
	"os"
	"strings"
	"sync"
	"time"
)

// Durable Queue, persistent in SQLite database
type DuraQueue struct {
	alive      *alive.Alive
	db         *sql.DB
	handle     HandleFunc
	lockStmt   *sql.Stmt
	pickupStmt *sql.Stmt
	pullAgain  *time.Timer
	pullStmt   *sql.Stmt
	pushStmt   *sql.Stmt
	qlk        sync.RWMutex
	queue      []*Task
	unlockStmt *sql.Stmt
	workerName string
}

type HandleFunc func(ctx context.Context, task *Task)

func configQueueSize(ctx context.Context) int { return junk.MustContextGetInt(ctx, "queue-size") }
func contextWithDbTimeoutFast(parent context.Context) context.Context {
	ctx, _ := context.WithTimeout(parent, junk.MustContextGetDuration(parent, "db-timeout-fast"))
	return ctx
}
func contextWithDbTimeoutMax(parent context.Context) context.Context {
	ctx, _ := context.WithTimeout(parent, junk.MustContextGetDuration(parent, "db-timeout-max"))
	return ctx
}

func (self *DuraQueue) Init(ctx context.Context, dbURL string, handle HandleFunc) {
	self.alive = alive.NewAlive()
	self.queue = make([]*Task, 0, configQueueSize(ctx))
	self.pullAgain = time.NewTimer(time.Minute)
	self.pullAgain.Stop()
	self.handle = handle

	if hostname, err := os.Hostname(); err != nil {
		log.Fatal("duraqueue._init os.Hostname() error:", err)
	} else {
		self.workerName = fmt.Sprintf("host:%s:pid:%d:started:%s",
			hostname, os.Getpid(), time.Now().UTC().Truncate(time.Minute).Format("2006-01-02T15:04"))
	}

	var err error
	dbDriver := "sqlite3"
	if junk.MustContextGetBool(ctx, "log-debug") {
		dbDriver = "sqlite3_tracing"
	}
	if self.db, err = sql.Open(dbDriver, dbURL); err != nil {
		log.Fatal(err)
	}
	initQueries := []string{
		`pragma case_sensitive_like = true`,
		`create table if not exists queue (
			id text primary key, created timestamp, modified timestamp, scheduled timestamp,
			locked timestamp, worker text, status text, msg_from text, msg_to text, msg_text text, data blob
		)`,
		`create index if not exists idx_queue_lock on queue (scheduled) where locked is null`,
		`create index if not exists idx_queue_pull on queue (locked, worker) where locked is not null`,
		`create table if not exists archive (
			id text primary key, created timestamp, modified timestamp, scheduled timestamp,
			locked timestamp, worker text, status text, msg_from text, msg_to text, msg_text text, data blob
		)`,
		`create index if not exists idx_archive_modified on archive (modified)`,
	}
	for _, query := range initQueries {
		junk.MustExec(ctx, self.db, query, "DuraQueue.Init create db schema ")
	}
	self.lockStmt = junk.MustPrepare(
		ctx, self.db, `update queue set locked = current_timestamp, worker = ?
	where (locked is null) and (scheduled < current_timestamp)
	order by scheduled
	limit ?`,
		"DuraQueue.Init db prepare queue lock statement ")
	self.pickupStmt = junk.MustPrepare(
		ctx, self.db, `select * from queue where locked < ? limit ?`,
		"DuraQueue.Init db prepare queue pickup statement ")
	self.pullStmt = junk.MustPrepare(
		ctx, self.db, `select * from queue where (locked is not null) and (worker = ?) limit ?`,
		"DuraQueue.Init db prepare queue pull statement ")
	self.unlockStmt = junk.MustPrepare(
		ctx, self.db, `update queue set modified = current_timestamp, locked = null, worker = null
	where (locked is not null) and (? like '% '||id||' %')`,
		"DuraQueue.Init db prepare queue unlock statement ")

	// 3 lines probably worth moving to a function to repeat stats info regularly
	totalQueueCount := junk.MustSelectInt(ctx, self.db, `select count(*) from queue`, "DuraQueue.Init db totalQueueCount ")
	lockedCount := junk.MustSelectInt(ctx, self.db, `select count(*) from queue where locked is not null`, "duraqueue.Init db lockedCount ")
	log.Printf("DuraQueue.Init db queue: %d locked of %d total", lockedCount, totalQueueCount)
}

func (self *DuraQueue) Run(ctx context.Context) {
	self.alive.Add(1) // prevent alive finished state until dbUnlock is done
	stopch := self.alive.StopChan()
	delayt := time.NewTimer(time.Hour)
	delayt.Stop()
	lastQueueSize := 0
	housekeep := time.NewTicker(17 * time.Minute)
	const delayMin = 3 * time.Second
	const delayMax = 13 * time.Second

runLoop:
	for {
		err := self.loadFromDB(ctx)
		if err != nil {
			log.Printf("duraqueue.Run LoadFromDB error: %s", err)
		}
		lastQueueSize = self.dispatch(ctx)
		queueLimit := configQueueSize(ctx)
		delayt.Reset(self.pullDelay(lastQueueSize, queueLimit, delayMin, delayMax))
		select {
		case <-stopch:
			break runLoop
		case <-delayt.C:
		case <-housekeep.C:
			junk.MustExec(ctx, self.db, `vacuum full`, "duraqueue.Run vacuum full")
		}
	}

	self.qlk.Lock()
	log.Printf("duraqueue.Run stopping with queue=%d", len(self.queue))
	unlockedCount := self.dbUnlock(ctx, self.queue)
	log.Printf("duraqueue.Run stopping unlocked %d", unlockedCount)
	self.alive.Done()
	self.qlk.Unlock()
}

func (self *DuraQueue) Stop() {
	self.alive.Stop()
}

func (self *DuraQueue) Wait() {
	self.alive.Wait()
}

func (self *DuraQueue) PushTalkSMSMessage(ctx context.Context, msg *bendertalk.SMSMessage) (string, error) {
	t := TaskFromTalk(msg)
	t.execer = self.db
	t.Worker = self.workerName
	err := t.Store(ctx)
	return t.Id, err
}

// lock scheduled and fetch into memory
func (self *DuraQueue) loadFromDB(ctx context.Context) error {
	self.qlk.Lock()
	defer self.qlk.Unlock()

	if len(self.queue) > 0 {
		log.Printf("BUG duraqueue.loadFromDB must deal with all items in queue before pulling new")
		return nil
	}

	limit := configQueueSize(ctx)
	pickupAge := junk.MustContextGetDuration(ctx, "duraqueue-pickup-age")

	// pick up items abandoned by a failed worker
	itemsLeft, err := TasksLoadPrepared(contextWithDbTimeoutFast(ctx), self.pickupStmt,
		time.Now().UTC().Add(-pickupAge), limit)
	if err != nil {
		log.Printf("duraqueue.loadFromDB pickup error: %s", err)
	} else {
		log.Printf("duraqueue.loadFromDB pickup %d", len(itemsLeft))
		self.dbUnlock(ctx, itemsLeft)
	}

	err = self.unsafeDBLock(ctx, limit)
	if err != nil {
		log.Printf("duraqueue.loadFromDB lock error: %s", err)
	}
	itemsNew, err := TasksLoadPrepared(contextWithDbTimeoutFast(ctx), self.pullStmt,
		self.workerName, limit)

	if err != nil {
		log.Printf("duraqueue.loadFromDB pull error: %s", err)
	}
	self.queue = append(self.queue, itemsNew...)

	log.Printf("debug duraqueue.loadFromDB loaded %d", len(self.queue))
	return err
}

func (self *DuraQueue) unsafeDBLock(ctx context.Context, limit int) error {
	r, err := self.lockStmt.ExecContext(contextWithDbTimeoutFast(ctx), self.workerName, limit)
	if err != nil {
		return err
	}
	n, err := r.RowsAffected()
	if err != nil {
		return err
	}
	log.Printf("debug duraqueue.QueueLock locked %d", n)
	return err
}

func (self *DuraQueue) dbUnlock(ctx context.Context, ts []*Task) int {
	if len(ts) == 0 {
		return 0
	}

	total := 0
	ids := make([]string, len(ts))
	for i, t := range ts {
		ids[i] = t.Id
	}
	joined := " " + strings.Join(ids, " ") + " "

	for try := 1; try <= 3; try++ {
		if try != 1 {
			time.Sleep(3 * time.Second)
		}

		r, err := self.unlockStmt.ExecContext(contextWithDbTimeoutFast(ctx), joined)
		if err == nil {
			if n, err := r.RowsAffected(); err == nil {
				total += int(n)
			}
			return total
		}
		log.Printf("duraqueue.dbUnlock db error: %s try=%d/3", err, try)
	}
	log.Printf("duraqueue.dbUnlock error was not resolved after 3 tries, so we stop")
	self.Stop()
	return total
}

// Calculate delay proportional to queue lastSize/limit mapped onto delayMin/Max scale.
// Delay is always 0 after queue was full.
func (self *DuraQueue) pullDelay(lastQueueSize, queueLimit int, min, max time.Duration) time.Duration {
	if lastQueueSize >= queueLimit {
		return 0
	}

	fillk := float32(lastQueueSize) / float32(queueLimit+1)
	if fillk > 1 {
		fillk = 1
	}
	rangeMilli := float32((max - min) / time.Millisecond)
	mapRange := rangeMilli * (1 - fillk)
	delay := min + time.Duration(mapRange)*time.Millisecond
	return delay
}

func (self *DuraQueue) dispatch(ctx context.Context) int {
	wg := sync.WaitGroup{}
	self.qlk.Lock()
	qSize := len(self.queue)
	for i, t := range self.queue {
		go self.handleWrap(ctx, t)
		self.queue[i] = nil
	}
	self.queue = self.queue[:0]
	self.qlk.Unlock()
	wg.Wait()
	return qSize
}

func (self *DuraQueue) handleWrap(ctx context.Context, t *Task) {
	t.execer = self.db
	self.handle(ctx, t)
}
