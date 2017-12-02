package junk

import (
	"context"
	"database/sql"
	"log"
	"time"
)

func MustPrepare(ctx context.Context, db *sql.DB, query string, errorPrefix string) *sql.Stmt {
	ctx, _ = context.WithDeadline(ctx, time.Now().Add(MustContextGetDuration(ctx, "db-timeout-fast")))
	stmt, err := db.PrepareContext(ctx, query)
	if err != nil {
		log.Fatalf("%serror: %s query:\n%s", errorPrefix, err, query)
	}
	return stmt
}

func MustExec(ctx context.Context, db *sql.DB, query string, errorPrefix string, args ...interface{}) (rows int, lastid int64) {
	ctx, _ = context.WithDeadline(ctx, time.Now().Add(MustContextGetDuration(ctx, "db-timeout-max")))
	r, err := db.ExecContext(ctx, query, args...)
	if err != nil {
		log.Fatalf("%serror: %s query:\n%s", errorPrefix, err, query)
	}
	if rows64, err := r.RowsAffected(); err == nil {
		rows = int(rows64)
	}
	if lid, err := r.LastInsertId(); err == nil {
		lastid = lid
	}
	return
}

func SelectInt(ctx context.Context, db *sql.DB, query string, args ...interface{}) (int, error) {
	ctx, _ = context.WithDeadline(ctx, time.Now().Add(MustContextGetDuration(ctx, "db-timeout-max")))
	r := db.QueryRowContext(ctx, query, args...)
	var result int
	if err := r.Scan(&result); err != nil {
		return 0, err
	}
	return result, nil
}

func MustSelectInt(ctx context.Context, db *sql.DB, query string, errorPrefix string, args ...interface{}) int {
	ctx, _ = context.WithDeadline(ctx, time.Now().Add(MustContextGetDuration(ctx, "db-timeout-max")))
	x, err := SelectInt(ctx, db, query, args...)
	if err != nil {
		log.Fatalf("%serror: %s query:\n%s", errorPrefix, err, query)
	}
	return x
}
