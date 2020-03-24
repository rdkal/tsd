package tsd_test

import (
	"fmt"
	"kal/tsd"
	"kal/tsd/arth"
	"testing"
	"time"
)

func Test_Query(t *testing.T) {
	q := tsd.Query{
		Series: "bitmex/trade",
		Where:  []string{"symbol == XBTUSD"},
		Project: []tsd.Projection{
			{
				F:  arth.Sum,
				Of: []string{"size"},
			},
			{
				F:  arth.VWAP,
				Of: []string{"size", "price"},
			},
		},
		From:   time.Date(2019, 05, 01, 00, 00, 00, 00, time.UTC),
		To:     time.Date(2019, 05, 01, 01, 00, 00, 00, time.UTC),
		Period: 1 * time.Minute,
	}

	iter := tsd.Exec(q)
	if err := iter.Err(); err == tsd.ErrNoRecords {
		return
	}
	type record struct {
		ts      time.Time
		symbols []string
		size    int
		vwap    float64
	}
	var recs []record
	for iter.Next() {
		var r record
		if err := iter.Scan(&r.size, &r.vwap); err != nil {
			t.Error(err)
		}
		r.ts = iter.Cursor()
		recs = append(recs, r)
	}
	if iter.Err() != nil {
		t.Error(iter.Err())
	}
	for _, r := range recs {
		fmt.Println(r.ts, r.size, r.vwap)
	}
}
