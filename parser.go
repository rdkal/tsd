package tsd

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strconv"
	"time"
)

var fieldSep = []byte{','}
var errFilter = errors.New("should filter")

type Input int

type op int

const (
	_ op = iota
	lt
	lte
	gt
	gte
	eq
)

type filter struct {
	op  op
	val interface{}
}

type parser struct {
	s bytesScanner
	// same length
	mask   []bool
	kind   []string
	layout []string
	buf    buffer
	filter []*filter

	err error

	from   time.Time
	to     time.Time
	cursor time.Time
	index  Input
}

type bytesScanner interface {
	Scan() bool
	Bytes() []byte
	Err() error
}

type parserConfig struct {
	Soruce bytesScanner
	From   time.Time
	To     time.Time
	Mask   []bool
	Filter []*filter
	Kind   []string
	Layout []string
	Index  Input
}

func newParser(cfg parserConfig) (*parser, error) {
	buf := make(buffer, len(cfg.Kind))
	for i := range cfg.Kind {
		switch cfg.Kind[i] {
		case "string":
			buf[i] = ""
		case "int":
			buf[i] = 0
		case "float64":
			buf[i] = 0.0
		case "time.Time":
			buf[i] = time.Time{}
		default:
			return nil, fmt.Errorf("tsd: supports time.Time | string | float64 | int, got %v", cfg.Kind[i])
		}
	}
	rec := &parser{
		s: cfg.Soruce,

		mask:   cfg.Mask,
		filter: cfg.Filter,
		kind:   cfg.Kind,
		layout: cfg.Layout,
		buf:    make([]interface{}, len(cfg.Mask)),

		from:   cfg.From,
		to:     cfg.To,
		cursor: cfg.From,
		index:  cfg.Index,
	}
	return rec, nil
}

// next poulates buf based
func (r *parser) Next() bool {
	if r.Err() != nil {
		return false
	}
	switch {
	case r.err == nil:
		if !r.s.Scan() {
			return false
		}
	case r.err == io.EOF:
		if r.cursor.Before(r.to) {
			r.err = nil
			return true
		} else {
			return false
		}
	}

	err := r.parse(r.s.Bytes())
	for err == errFilter {
		if !r.s.Scan() {
			return false
		}
		err = r.parse(r.s.Bytes())
	}
	if err != nil {
		r.err = err
		return false
	}
	return true
}

func (r *parser) Until(t time.Time) {
	if r.Err() != nil {
		return
	}
	if r.to.After(t) {
		r.err = fmt.Errorf("tsd: %v is after %v, shuld be before", r.to, t)
		return
	}
	r.to = t
}

func (r *parser) parse(bb []byte) error {
	fields := bytes.Split(r.s.Bytes(), []byte{','})

	for i, mask := range r.mask {
		if !mask {
			continue
		}
		bb := fields[i]
		switch r.kind[i] {
		case "time.Time":
			t, err := time.Parse(r.layout[i], string(bb))
			if err != nil {
				r.err = err
				return err
			}
			if r.filter[i] != nil && !r.filterTime(t, r.filter[i]) {
				return errFilter
			}
			r.buf[i] = t
			if t.Equal(r.to) || t.After(r.to) {
				r.err = io.EOF
			}
			r.cursor = t
		case "string":
			if r.filter[i] != nil && !r.filterString(string(bb), r.filter[i]) {
				return errFilter
			}
			r.buf[i] = string(bb)
		case "int":
			n, err := strconv.Atoi(string(bb))
			if err != nil {
				r.err = err
				return r.err
			}
			if r.filter[i] != nil && !r.filterInt(n, r.filter[i]) {
				return errFilter
			}
			r.buf[i] = n
		case "float64":
			n, err := strconv.ParseFloat(string(bb), 64)
			if err != nil {
				r.err = err
				return r.err
			}
			if r.filter[i] != nil && !r.filterFloat64(n, r.filter[i]) {
				return errFilter
			}
			r.buf[i] = n
		default:
			r.err = fmt.Errorf("tsd: supports time.Time | string | float64 | int, got %v", r.kind[i])
		}
	}
	return r.err
}

func (r *parser) filterTime(t time.Time, f *filter) bool {
	val := f.val.(time.Time)
	switch f.op {
	case lt:
		return t.Before(val)
	case lte:
		return t.Before(val) || t.Equal(val)
	case gt:
		return t.After(val)
	case gte:
		return t.After(val) || t.Equal(val)
	case eq:
		return t.Equal(val)
	}
	return false
}

func (r *parser) filterInt(n int, f *filter) bool {
	val := f.val.(int)
	switch f.op {
	case lt:
		return n < val
	case lte:
		return n <= val
	case gt:
		return n > val
	case gte:
		return n >= val
	case eq:
		return n == val
	}
	return false
}

func (r *parser) filterFloat64(n float64, f *filter) bool {
	val := f.val.(float64)
	switch f.op {
	case lt:
		return n < val
	case lte:
		return n <= val
	case gt:
		return n > val
	case gte:
		return n >= val
	case eq:
		return n == val
	}
	return false
}

func (r *parser) filterString(str string, f *filter) bool {
	val := f.val.(string)
	switch f.op {
	case lt:
		return str < val
	case lte:
		return str <= val
	case gt:
		return str > val
	case gte:
		return str >= val
	case eq:
		return str == val
	}
	return false
}

func (r *parser) Scan(dest ...interface{}) error {
	if r.Err() != nil {
		return r.err
	}
	if len(dest) != len(r.buf) {
		r.err = fmt.Errorf("tsd: dest, buf length mismatch")
		return r.err
	}
	for i, mask := range r.mask {
		if !mask {
			dest[i] = nil
			continue
		}
		if err := assign(dest[i], r.buf[i]); err != nil {
			r.err = fmt.Errorf("parser: field %v, %v", i, err)
			return r.err
		}
	}
	return nil
}

func (r *parser) Err() error {
	if r.err != nil && r.err != io.EOF {
		return r.err
	}
	return nil
}

func dependsOn(projs []projection) []Input {
	needSet := make(map[Input]bool)
	for _, p := range projs {
		for _, input := range p.Of {
			needSet[input] = true
		}
	}
	var need []Input
	for input := range needSet {
		need = append(need, input)
	}
	return need
}
