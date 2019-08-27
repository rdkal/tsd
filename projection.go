package tsd

import (
	"fmt"
	"kal/tsd/arth"
	"time"
)

type Projection struct {
	F  arth.ID
	Of []string
}

type projection struct {
	F arth.ID

	// same length
	Of     []Input
	inputs []interface{}
}

func (p projection) ok() error {
	if p.F < 0 {
		return fmt.Errorf("invalid arth %v should be >= 0", p.F)
	}
	if p.F == 0 && len(p.Of) != 1 {
		return fmt.Errorf("F=assign, expected 1 arguemnt got %v", p.Of)
	}
	if len(p.Of) != len(p.inputs) {
		return fmt.Errorf("len(p.Of) != len(p.inputs) %v != %v ", len(p.Of), len(p.inputs))
	}
	return nil
}

func (proj projection) exec(output interface{}, buf buffer) error {
	if proj.F == 0 {
		return assign(output, buf.get(proj.Of[0]))
	} else {
		for i := range proj.inputs {
			proj.inputs[i] = buf.get(proj.Of[i])
		}
		return arth.Exec(proj.F, output, proj.inputs...)
	}
}

func assign(dest interface{}, src interface{}) error {
	switch d := dest.(type) {
	case *time.Time:
		s, ok := src.(time.Time)
		if !ok {
			return &errTypeMismatch{src: src, dest: dest, op: "assign"}
		}
		*d = s
	case *string:
		s, ok := src.(string)
		if !ok {
			return &errTypeMismatch{src: src, dest: dest, op: "assign"}
		}
		*d = s
	case *int:
		s, ok := src.(int)
		if !ok {
			return &errTypeMismatch{src: src, dest: dest, op: "assign"}
		}
		*d = s
	case *float64:
		s, ok := src.(float64)
		if !ok {
			return &errTypeMismatch{src: src, dest: dest, op: "assign"}
		}
		*d = s
	default:
		return &errTypeMismatch{src: src, dest: dest, op: "assign"}
	}
	return nil
}
