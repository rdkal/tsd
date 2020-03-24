package tsd

import (
	"bufio"
	"strings"
	"testing"
	"time"
)

func Test_Parse(t *testing.T) {
	pp, err := newParser(parserConfig{
		Soruce: bufio.NewScanner(strings.NewReader(data)),
		From:   time.Date(2019, 5, 1, 00, 00, 00, 0, time.UTC),
		To:     time.Date(2019, 5, 1, 00, 00, 8, 0, time.UTC),
		Filter: []*filter{nil, nil, nil, nil, nil},
		Mask:   []bool{true, true, false, true, true},
		Kind:   []string{"time.Time", "string", "string", "int", "float64"},
		Layout: []string{"2006-01-02D15:04:05.999999999"},
	})
	if err != nil {
		t.Error(err)
	}
	type record struct {
		timestamp time.Time
		symbol    string
		size      int
		price     float64
	}
	var rec record
	var rr []record
	n := 0
	for pp.Next() {
		err := pp.Scan(&rec.timestamp, &rec.symbol, nil, &rec.size, &rec.price)
		if err != nil {
			t.Error(err)
		}
		rr = append(rr, rec)
		n++
	}
	if err := pp.Err(); err != nil {
		t.Error("got", err, "want", nil)
	}
	if n != 5 {
		t.Error("got", n, "results", "want", 5)
	}

	n = 0
	pp.Until(time.Date(2019, 5, 1, 00, 00, 10, 0, time.UTC))
	for pp.Next() {
		err := pp.Scan(&rec.timestamp, &rec.symbol, nil, &rec.size, &rec.price)
		if err != nil {
			t.Error(err)
		}
		rr = append(rr, rec)
		n++
	}
	if err := pp.Err(); err != nil {
		t.Error(err)
	}
	if n != 4 {
		t.Error("got", n, "results", "want", 4)
	}
	if rr[0].size != 900 {
		t.Error()
	}
	want, err := time.Parse("2006-01-02D15:04:05.999999999", "2019-05-01D00:00:07.733633000")
	if err != nil {
		t.Error(err)
	}
	if rr[0].timestamp != want {
		t.Errorf("got %v want %v", rr[0].timestamp, want)
	}
	want, err = time.Parse("2006-01-02D15:04:05.999999999", "2019-05-01D00:00:08.542868000")
	if err != nil {
		t.Error(err)
	}
	got := rr[len(rr)-1].timestamp
	if got != want {
		t.Errorf("got %v want %v", rr[0].timestamp, want)
	}
	if rr[len(rr)-1].size != 2675 {
		t.Error("")
	}
}
