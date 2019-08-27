package tsd

import (
	"bufio"
	"kal/tsd/arth"
	"strings"
	"testing"
	"time"
)

func Test_Record(t *testing.T) {
	From := time.Date(2019, 5, 1, 00, 00, 0, 0, time.UTC)
	To := time.Date(2019, 5, 1, 00, 00, 11, 0, time.UTC)
	Mask := []bool{true, false, false, true, false}
	Filter := []*filter{nil, nil, nil, nil, nil}
	Kind := []string{"time.Time", "string", "string", "int", "float64"}
	Layout := []string{"2006-01-02D15:04:05.999999999"}
	// scanner, err := newMulScanner(len(Kind), ioutil.NopCloser(strings.NewReader(data)))
	// if err != nil {
	// t.Error(err)
	// }

	pp, err := newParser(parserConfig{
		Soruce: bufio.NewScanner(strings.NewReader(data)),
		From:   From,
		To:     From,
		Mask:   Mask,
		Filter: Filter,
		Kind:   Kind,
		Layout: Layout,
	})
	if err != nil {
		t.Error(err)
	}
	projs := []projection{
		{
			F:      arth.Sum,
			Of:     []Input{3},
			inputs: []interface{}{struct{}{}},
		},
	}
	for _, p := range projs {
		if err := p.ok(); err != nil {
			t.Error(err)
		}
	}

	recs := &records{
		pp:     pp,
		proj:   projs,
		kind:   Kind,
		mask:   Mask,
		from:   From,
		to:     To,
		cursor: From,
		period: 1 * time.Second,
	}
	if err := recs.initBuffer(); err != nil {
		t.Error(err)
	}
	type record struct {
		timestamp time.Time
		size      int
	}
	n := 0

	var rows []record
	for recs.Next() {
		var r record
		r.timestamp = recs.Cursor()
		err := recs.Scan(&r.size)
		if err != nil {
			t.Error(err)
		}
		rows = append(rows, r)
		n++
	}
	if recs.Err() != nil {
		t.Error(recs.Err())
	}
	if n == 0 {
		t.Error("No results")
	}
}

// timestamp,symbol,side,size,price,tickDirection,trdMatchID,grossValue,homeNotional,foreignNotional
const data = `2019-05-01D00:00:07.733633000,ADAM19,Buy,900,1.347e-05,PlusTick,bd386f13-96a3-54d3-7532-6af87eefaffa,1212300,900,0.012123
2019-05-01D00:00:07.733633000,ADAM19,Buy,916,1.347e-05,ZeroPlusTick,3b59e8cd-3746-9181-b213-a028e9e6b7f2,1233852,916,0.01233852
2019-05-01D00:00:07.733633000,ADAM19,Buy,223,1.347e-05,ZeroPlusTick,38e4f993-ebe0-0b4a-1eca-fce1269b209a,300381,223,0.00300381
2019-05-01D00:00:07.733633000,ADAM19,Buy,600,1.347e-05,ZeroPlusTick,63179ab6-8d8b-1bdc-7c30-14de14355dd2,808200,600,0.008082
2019-05-01D00:00:07.733633000,ADAM19,Buy,644,1.347e-05,ZeroPlusTick,911b6b15-e451-8e6f-d2f6-53ad1da4f805,867468,644,0.00867468
2019-05-01D00:00:08.542868000,ADAM19,Buy,9356,1.347e-05,ZeroPlusTick,64c4c7e7-b7b9-ac39-2049-4bea02cd4b69,12602532,9356,0.1260253
2019-05-01D00:00:08.542868000,ADAM19,Buy,1,1.347e-05,ZeroPlusTick,b5b0ede3-deee-9a07-e889-bd3e1bcacf0f,1347,1,1.347e-05
2019-05-01D00:00:08.542868000,ADAM19,Buy,3638,1.347e-05,ZeroPlusTick,50b04b0f-8230-ae7e-0de5-fedd622fa334,4900386,3638,0.04900386
2019-05-01D00:00:08.542868000,ADAM19,Buy,2675,1.347e-05,ZeroPlusTick,89d915c4-d37d-4e5a-af3f-9a583fe31b2a,3603225,2675,0.03603225
`
