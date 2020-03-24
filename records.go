package tsd

import (
	"fmt"
	"time"
)

type records struct {
	pp    *parser
	index int

	proj []projection

	// length of Input
	group  buffer
	record buffer
	kind   []string
	mask   []bool

	from   time.Time
	to     time.Time
	cursor time.Time

	period time.Duration

	err   error
	close func() error
}

func (r *records) Next() bool {
	if r.err != nil {
		return false
	}
	if r.cursor.After(r.to) || r.cursor.Equal(r.to) {
		return false
	}
	r.group.reset()
	r.cursor = r.cursor.Add(r.period).Truncate(r.period)
	r.pp.Until(r.cursor)
	for r.pp.Next() {
		if r.err = r.pp.Scan(r.record...); r.err != nil {
			return false
		}
		for i, val := range r.record {
			if val == nil {
				continue
			}
			if r.err = r.group.append(Input(i), val); r.err != nil {
				return false
			}
		}
	}
	if r.err = r.pp.Err(); r.err != nil {
		return false
	}
	return true
}
func (r *records) Scan(dest ...interface{}) error {
	if r.err != nil {
		return r.err
	}

	if len(dest) != len(r.proj) {
		r.err = fmt.Errorf("tsd: expected len(dest) = len(projections), %v != %v", len(dest), len(r.proj))
		return r.err
	}
	for i, proj := range r.proj {
		if r.err = proj.exec(dest[i], r.group); r.err != nil {
			return r.err
		}
	}
	return nil
}
func (r *records) Err() error {
	if r.err == nil {
		return r.pp.Err()
	}
	return r.err
}
func (r *records) Close() error {
	if r.close != nil {
		return r.close()
	}
	return nil
}

// Cursor returns the begninning of the current period
func (r *records) Cursor() time.Time {
	return r.cursor.Add(-r.period)
}

func (r *records) initBuffer() error {
	r.group = make(buffer, len(r.kind))
	r.record = make(buffer, len(r.kind))
	for i := range r.kind {
		switch r.kind[i] {
		case "time.Time":
			r.group[i] = make([]time.Time, 0, defualtGroupCapasity)
			r.record[i] = &time.Time{}
		case "string":
			r.group[i] = make([]string, 0, defualtGroupCapasity)
			var str string = ""
			r.record[i] = &str
		case "int":
			var n int
			r.group[i] = make([]int, 0, defualtGroupCapasity)
			r.record[i] = &n
		case "float64":
			r.group[i] = make([]float64, 0, defualtGroupCapasity)
			var n float64
			r.record[i] = &n
		default:
			r.err = fmt.Errorf("tsd: supports time.Time | string | float64 | int, got %v", r.kind[i])
			return r.err
		}
	}
	return nil
}
