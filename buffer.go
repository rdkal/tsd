package tsd

import (
	"fmt"
	"time"
)

type errTypeMismatch struct {
	dest interface{}
	op   string
	src  interface{}
}

func (err *errTypeMismatch) Error() string {
	return fmt.Sprintf("can't %v %T to %T", err.op, err.src, err.dest)
}

type buffer []interface{}

func (buf buffer) get(i Input) interface{} { return buf[i] }

func (buf buffer) append(input Input, val interface{}) error {
	switch bb := buf[input].(type) {
	case []time.Time:
		v, ok := val.(*time.Time)
		if !ok {
			return &errTypeMismatch{val, "append", buf[input]}
		}
		bb = append(bb, *v)
		buf[input] = bb
	case []string:
		v, ok := val.(*string)
		if !ok {
			return &errTypeMismatch{val, "append", buf[input]}
		}
		bb = append(bb, *v)
		buf[input] = bb
	case []int:
		v, ok := val.(*int)
		if !ok {
			return &errTypeMismatch{val, "append", buf[input]}
		}
		bb = append(bb, *v)
		buf[input] = bb
	case []float64:
		v, ok := val.(*float64)
		if !ok {
			return &errTypeMismatch{val, "append", buf[input]}
		}
		bb = append(bb, *v)
		buf[input] = bb
	default:
		return &errTypeMismatch{val, "append", buf[input]}
	}
	return nil
}

func (buf buffer) reset() {
	for i := range buf {
		switch bb := buf[i].(type) {
		case []string:
			buf[i] = bb[:0]
		case []int:
			buf[i] = bb[:0]
		case []float64:
			buf[i] = bb[:0]
		case []time.Time:
			buf[i] = bb[:0]
		}

	}
}

const defualtGroupCapasity = 10
