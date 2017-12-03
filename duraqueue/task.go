package duraqueue

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/temoto/senderbender/talk"
	"log"
	"strings"
	"time"
)

type DBExecer interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
}

type Task struct {
	Id        string
	Created   time.Time
	Modified  time.Time
	Scheduled time.Time
	Locked    time.Time
	Worker    string
	Status    string
	MsgFrom   string
	MsgTo     string
	MsgText   string
	Data      []byte

	execer DBExecer
}

func (self Task) String() string {
	now := time.Now().UTC()
	created := "null"
	if !self.Created.IsZero() {
		created = now.Sub(self.Created).Truncate(time.Second).String()
	}
	modified := "null"
	if !self.Modified.IsZero() {
		modified = now.Sub(self.Modified).Truncate(time.Second).String()
	}
	sched := "not"
	if self.Scheduled.Unix() == 1 {
		sched = "asap"
	} else if !self.Scheduled.IsZero() {
		sched = now.Sub(self.Scheduled).Truncate(time.Second).String()
	}
	locked := "not"
	if !self.Locked.IsZero() {
		locked = now.Sub(self.Locked).Truncate(time.Second).String()
	}
	return fmt.Sprintf("id=%s status=%s created=%s mod=%s sched=%s locked=%s worker=%s from=%s to=%s text=%s data=%db",
		self.Id, self.Status, created, modified, sched, locked, self.Worker, self.MsgFrom, self.MsgTo, self.MsgText, len(self.Data),
	)
}

func (self *Task) ToSMSMessage() (*bendertalk.SMSMessage, error) {
	msg := new(bendertalk.SMSMessage)
	if err := msg.Unmarshal(self.Data); err != nil {
		return nil, err
	}
	return msg, nil
}

func TalkStateToStatus(state bendertalk.SMPPMessageState) string {
	return strings.Replace(state.String(), "SMPPMessageState", "", 1)
}

func TaskFromSMSMessage(msg *bendertalk.SMSMessage) *Task {
	msgBytes, err := msg.Marshal()
	if err != nil {
		log.Fatalf("bend.TaskFromData msg.Marshal error: %s msg=%#v", err, msg)
		return nil
	}
	now := time.Now().UTC().Truncate(time.Second)
	self := &Task{
		Id:       msg.Id,
		Created:  now,
		Modified: now,
		MsgFrom:  msg.From,
		MsgTo:    msg.To,
		MsgText:  msg.Text,
		Data:     msgBytes,
	}
	if msg.ScheduleUnix != 0 {
		self.Scheduled = time.Unix(msg.ScheduleUnix, 0).UTC()
	}
	self.Status = TalkStateToStatus(msg.State)
	return self
}

func TasksLoadPrepared(ctx context.Context, stmt *sql.Stmt, args ...interface{}) ([]*Task, error) {
	rows, err := stmt.QueryContext(ctx, args...)
	if err != nil {
		return nil, err
	}
	ts, err := tasksFromRows(rows, configQueueSize(ctx)*2)
	rows.Close()
	return ts, err
}

func (self *Task) Store(ctx context.Context) error {
	if self.execer == nil {
		log.Fatal("BUG duraqueue.Task.Store is called without execer field. task=%s", self)
		return nil
	}

	// FIXME: remove excess marshal/unmarhsal
	if msg, _ := self.ToSMSMessage(); msg != nil {
		self.Status = TalkStateToStatus(msg.State)
	}
	nullLocked := &self.Locked
	if self.Locked.IsZero() {
		nullLocked = nil
	}
	nullScheduled := &self.Scheduled
	if self.Scheduled.IsZero() {
		nullScheduled = nil
	}

	var err error
	for try := 1; try <= 3; try++ {
		if try != 1 {
			time.Sleep(3 * time.Second)
		}

		_, err = self.execer.ExecContext(
			contextWithDbTimeoutFast(ctx),
			`update queue set modified = ?, scheduled = ?, locked = ?, worker = ?, status = ?, data = ? where id = ?;
			insert into queue (id, created, modified, scheduled, locked, worker, status, msg_from, msg_to, msg_text, data)
				select ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
				where (select Changes() = 0);`,
			self.Modified, nullScheduled, nullLocked, self.Worker, self.Status, self.Data, self.Id,
			self.Id, self.Created, self.Modified, nullScheduled, nullLocked, self.Worker, self.Status, self.MsgFrom, self.MsgTo, self.MsgText, self.Data,
		)
		if err == nil {
			return nil
		}
		log.Printf("duraqueue.Store db error: %s try=%d/3", err, try)
	}
	log.Fatalf("duraqueue.Store db error was not resolved after 3 tries so we stop")
	return err
}

func (self *Task) Archive(ctx context.Context) error {
	if self.execer == nil {
		log.Fatal("BUG duraqueue.Task.Archive is called without execer field. task=%s", self)
		return nil
	}

	self.Modified = time.Now().UTC().Truncate(time.Second)
	self.Locked = time.Time{}
	// FIXME: remove excess marshal/unmarhsal
	if msg, _ := self.ToSMSMessage(); msg != nil {
		self.Status = TalkStateToStatus(msg.State)
	}

	var err error
	for try := 1; try <= 3; try++ {
		if try != 1 {
			time.Sleep(3 * time.Second)
		}

		_, err = self.execer.ExecContext(
			contextWithDbTimeoutFast(ctx),
			`insert or replace into archive (id, created, modified, scheduled, worker, status, msg_from, msg_to, msg_text, data)
				values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
			delete from queue where id = ?;`,
			self.Id, self.Created, self.Modified, self.Scheduled, self.Worker, self.Status, self.MsgFrom, self.MsgTo, self.MsgText, self.Data,
			self.Id,
		)
		if err == nil {
			return nil
		}
		log.Printf("duraqueue.Archive db error: %s try=%d/3", err, try)
	}
	log.Fatalf("duraqueue.Archive db error was not resolved after 3 tries so we stop")
	return err
}

func tasksFromRows(rows *sql.Rows, allocateLength int) ([]*Task, error) {
	result := make([]*Task, 0, allocateLength)
	columns, err := rows.Columns()
	if err != nil {
		log.Fatal("duraqueue.tasksFromRows Columns() error: %s", err)
	}
	for rows.Next() {
		self := new(Task)
		if len(columns) == 1 {
			err = rows.Scan(&self.Id)
		} else {
			err = rows.Scan(&self.Id, &self.Created, &self.Modified, &self.Scheduled, &self.Locked,
				&self.Worker, &self.Status, &self.MsgFrom, &self.MsgTo, &self.MsgText, &self.Data)
		}
		if err != nil {
			log.Fatalf("BUG duraqueue.tasksFromRows rows.Scan error: %s", err)
		}
		// log.Printf("debug duraqueue.tasksFromRows row: %s", self)
		result = append(result, self)
	}
	err = rows.Err()
	return result, err
}
