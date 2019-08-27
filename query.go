package tsd

import (
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Query struct {
	Series  string
	Where   []string
	Project []Projection
	From    time.Time
	To      time.Time
	Period  time.Duration

	// same length
	files []string
	dates []time.Time

	index Input

	fields map[string]fieldInfo
	names  []string
	kinds  []string
	path   string
	base   *records

	fileHandlers []*os.File

	once *sync.Once
	err  error
}

type Iter interface {
	Next() bool
	Scan(dest ...interface{}) error
	Err() error
	Close() error
	Cursor() time.Time
}

func Exec(q Query) Iter {

	rec, err := exec(q)
	if err != nil {
		return &records{err: fmt.Errorf("tsd: %v", err)}
	}
	if err := rec.initBuffer(); err != nil {
		return &records{err: fmt.Errorf("tsd: %v", err)}
	}
	return rec
}

func exec(q Query) (*records, error) {
	q.once = &sync.Once{}
	s, err := newSeries(q.Series)
	if err != nil {
		return nil, err
	}

	q.fields = s.Fields
	q.files = s.Files
	q.kinds = s.Kinds
	q.names = s.Names
	q.dates = s.Dates
	q.index = s.Fields[s.Index].Pos

	if err := q.validate(); err != nil {
		return nil, err
	}
	records := q.makeRecord()
	return records, q.err
}

func (q *Query) makeRecord() *records {
	if q.err != nil {
		return nil
	}
	rec := &records{
		pp:     q.makeParser(),
		proj:   q.makeProjection(),
		kind:   q.kinds,
		mask:   q.makeMask(),
		from:   q.From,
		to:     q.To,
		cursor: q.From,
		period: q.Period,
	}
	if q.err != nil {
		return nil
	}
	if rec.Err() != nil {
		q.once.Do(func() { q.err = rec.err })
		return nil
	}
	return rec
}

func (q *Query) makeMask() []bool {
	mask := make([]bool, len(q.kinds))
	for _, proj := range q.Project {
		for _, of := range proj.Of {
			mask[q.fields[of].Pos] = true
		}
	}
	for _, where := range q.Where {
		words := strings.Fields(where)
		info, ok := q.fields[words[0]]
		if !ok {
			return nil
		}
		mask[info.Pos] = true
	}
	mask[q.index] = true
	return mask
}

func (q *Query) makeParser() *parser {
	if q.err != nil {
		return nil
	}

	var layout []string

	for _, name := range q.names {
		info, ok := q.fields[name]
		if !ok {
			q.once.Do(func() { q.err = fmt.Errorf("expected %v to be in db", name) })
		}
		layout = append(layout, info.Layout)
	}
	pp, err := newParser(parserConfig{
		Soruce: q.makeScanner(),
		From:   q.From,
		To:     q.From,
		Kind:   q.kinds,
		Mask:   q.makeMask(),
		Filter: q.makeFilter(),
		Layout: layout,
		Index:  q.index,
	})
	if err != nil {
		q.once.Do(func() { q.err = err })
		return nil
	}
	return pp
}

func (q *Query) makeFilter() []*filter {
	filters := make([]*filter, len(q.kinds))
	for _, where := range q.Where {
		words := strings.Fields(where)
		if len(words) != 3 {
			q.once.Do(func() { q.err = fmt.Errorf("where clause \"%v\" has to have three words", where) })
			return nil
		}
		info, ok := q.fields[words[0]]
		if !ok {
			q.once.Do(func() { q.err = fmt.Errorf("where caluse \"%v\": no field named %v", where, words[0]) })
			return nil
		}
		var op op
		switch words[1] {
		case "<":
			op = lt
		case "<=":
			op = lte
		case ">":
			op = gt
		case ">=":
			op = gte
		case "==":
			op = eq
		default:
			q.once.Do(func() { q.err = fmt.Errorf("where clause \"%v\": unknown op %v", where, words[1]) })
			return nil
		}
		var val interface{}
		switch info.Kind {
		case "time.Time":
			t, err := time.Parse(time.RFC3339Nano, words[2])
			if err != nil {
				q.once.Do(func() { q.err = err })
				return nil
			}
			val = t
		case "string":
			val = words[2]
		case "int":
			n, err := strconv.Atoi(words[2])
			if err != nil {
				q.once.Do(func() { q.err = err })
				return nil
			}
			val = n
		case "float64":
			n, err := strconv.ParseFloat(words[2], 64)
			if err != nil {
				q.once.Do(func() { q.err = err })
				return nil
			}
			val = n
		default:
			q.once.Do(func() { q.err = fmt.Errorf("bad kind: %v", info.Kind) })
			return nil
		}
		filters[info.Pos] = &filter{
			op:  op,
			val: val,
		}
	}
	return filters
}

func (q *Query) makeScanner() *mulScanner {
	if q.err != nil {
		return nil
	}
	var files []io.ReadCloser
	for i, date := range q.dates {
		if (date.Equal(q.From) || date.After(q.From)) && date.Before(q.To) {
			file, err := os.Open(q.files[i])
			if err != nil {
				q.once.Do(func() { q.err = err })
				return nil
			}
			if strings.HasSuffix(file.Name(), ".gz") {
				gz, err := gzip.NewReader(file)
				if err != nil {
					q.once.Do(func() { q.err = err })
					return nil
				}
				files = append(files, gz)
			} else {
				files = append(files, file)
			}
		}
	}

	scanner, err := newMulScanner(len(q.kinds), files...)
	if err != nil {
		q.once.Do(func() { q.err = err })
		return nil
	}
	return scanner
}

func (q *Query) makeProjection() []projection {
	var _projs []projection
	for _, proj := range q.Project {
		var _of []Input
		for _, of := range proj.Of {
			_of = append(_of, q.fields[of].Pos)
		}
		_proj := projection{
			F:      proj.F,
			Of:     _of,
			inputs: make([]interface{}, len(_of)),
		}
		_projs = append(_projs, _proj)
	}
	return _projs
}

func (q *Query) validate() error {
	for _, proj := range q.Project {
		for _, of := range proj.Of {
			if _, ok := q.fields[of]; !ok {
				return fmt.Errorf("field %v does not exist in %v, choose from %v", proj, q.Series, q.fields)
			}
		}
	}
	return nil
}
