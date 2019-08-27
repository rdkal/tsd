package tsd

import (
	"fmt"
	"time"
)

type buffer []interface{}

func (buf buffer) get(i Input) interface{} { return buf[i] }

func (buf buffer) append(input Input, val interface{}) error {
	err := fmt.Errorf("can't append %T to %T", val, buf[input])
	switch bb := buf[input].(type) {
	case []time.Time:
		v, ok := val.(*time.Time)
		if !ok {
			return err
		}
		bb = append(bb, *v)
		buf[input] = bb
	case []string:
		v, ok := val.(*string)
		if !ok {
			return err
		}
		bb = append(bb, *v)
		buf[input] = bb
	case []int:
		v, ok := val.(*int)
		if !ok {
			return err
		}
		bb = append(bb, *v)
		buf[input] = bb
	case []float64:
		v, ok := val.(*float64)
		if !ok {
			return err
		}
		bb = append(bb, *v)
		buf[input] = bb
	default:
		return err
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
