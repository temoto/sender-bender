// +build trace

package duraqueue

import (
	"database/sql"
	"fmt"
	"github.com/temoto/go-sqlite3"
	"log"
)

func sqliteTraceCallback(info sqlite3.TraceInfo) int {
	// Not very readable but may be useful; uncomment next line in case of doubt:
	// log.Printf("sqlite_trace: %#v\n", info)

	var dbErrText string
	if info.DBError.Code != 0 || info.DBError.ExtendedCode != 0 {
		dbErrText = fmt.Sprintf("; DB error: %#v", info.DBError)
	} else {
		dbErrText = "."
	}

	// Show the Statement-or-Trigger text in curly braces ('{', '}')
	// since from the *paired* ASCII characters they are
	// the least used in SQL syntax, therefore better visual delimiters.
	// Maybe show 'ExpandedSQL' the same way as 'StmtOrTrigger'.
	//
	// A known use of curly braces (outside strings) is
	// for ODBC escape sequences. Not likely to appear here.
	//
	// Template languages, etc. don't matter, we should see their *result*
	// at *this* level.
	// Strange curly braces in SQL code that reached the database driver
	// suggest that there is a bug in the application.
	// The braces are likely to be either template syntax or
	// a programming language's string interpolation syntax.

	var expandedText string
	if info.ExpandedSQL != "" {
		if info.ExpandedSQL == info.StmtOrTrigger {
			expandedText = " = exp"
		} else {
			expandedText = fmt.Sprintf(" expanded {%q}", info.ExpandedSQL)
		}
	} else {
		expandedText = ""
	}

	// SQLite docs as of September 6, 2016: Tracing and Profiling Functions
	// https://www.sqlite.org/c3ref/profile.html
	//
	// The profile callback time is in units of nanoseconds, however
	// the current implementation is only capable of millisecond resolution
	// so the six least significant digits in the time are meaningless.
	// Future versions of SQLite might provide greater resolution on the profiler callback.

	var runTimeText string
	if info.RunTimeNanosec == 0 {
		if info.EventCode == sqlite3.TraceProfile {
			runTimeText = "; time 0" // no measurement unit
		}
	} else {
		const nanosPerMillisec = 1000000
		if info.RunTimeNanosec%nanosPerMillisec == 0 {
			runTimeText = fmt.Sprintf("; time %d ms", info.RunTimeNanosec/nanosPerMillisec)
		}
	}

	var modeText string
	if info.AutoCommit {
		modeText = "-AC-"
	} else {
		modeText = "+Tx+"
	}

	log.Printf("sqlite_trace: ev %d %s conn 0x%x, stmt 0x%x {%q}%s%s%s\n",
		info.EventCode, modeText, info.ConnHandle, info.StmtHandle,
		info.StmtOrTrigger, expandedText,
		runTimeText,
		dbErrText)
	return 0
}

func init() {
	sql.Register("sqlite3_tracing",
		&sqlite3.SQLiteDriver{
			ConnectHook: func(conn *sqlite3.SQLiteConn) error {
				err := conn.SetTrace(&sqlite3.TraceConfig{
					Callback:        sqliteTraceCallback,
					EventMask:       sqlite3.TraceStmt | sqlite3.TraceProfile | sqlite3.TraceRow | sqlite3.TraceClose,
					WantExpandedSQL: true,
				})
				return err
			},
		})
}
