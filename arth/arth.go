package arth

import (
	"fmt"
	"time"
)

type ID int

const (
	_ ID = iota
	Sum
	VWAP // VWAP(volume, price)
	First
	Collect
	Copy
)

var ErrInputLength = fmt.Errorf("arth: wrong number of inputs")

func Exec(f ID, output interface{}, input ...interface{}) error {
	switch f {
	case Sum:
		if len(input) != 1 {
			return ErrInputLength
		}
		switch nn := input[0].(type) {
		case []int:
			a := output.(*int)
			sumInt(a, nn)
		case []float64:
			a := output.(*float64)
			sumFloat(a, nn)
		default:
			return fmt.Errorf("can only sum over []int | []float64, got %T", nn)
		}
	case VWAP:
		if len(input) != 2 {
			return ErrInputLength
		}
		volumes := input[0].([]int)
		prices := input[1].([]float64)
		if len(volumes) == 0 {
			return fmt.Errorf("arth: empty input to vwap")
		}
		a := output.(*float64)
		vwap(a, volumes, prices)
	case First:
		switch nn := input[0].(type) {
		case []string:
			a := output.(*string)
			if len(nn) == 0 {
				*a = ""
				return nil
			}
			*a = nn[0]
		case []int:
		case []time.Time:
		case []float64:
		}
	case Collect:
		if err := collect(output, input...); err != nil {
			return err
		}
	case Copy:
		if err := copy(output, input[0]); err != nil {
			return err
		}
	default:
		panic("arth: unkown function")
	}
	return nil
}

func collect(output interface{}, input ...interface{}) error {
	switch in := input[0].(type) {
	case time.Time:
		out, ok := output.(*[]time.Time)
		if !ok {
			return fmt.Errorf("arth: output should be *[]time.Time | *[]string | *[]float64 | *[]int, got %T", input[0])
		}
		*out = append(*out, in)
	case string:
		out, ok := output.(*[]string)
		if !ok {
			return fmt.Errorf("arth: output should be *[]time.Time | *[]string | *[]float64 | *[]int, got %T", input[0])
		}
		*out = append(*out, in)
	case int:
		out, ok := output.(*[]int)
		if !ok {
			return fmt.Errorf("arth: output should be *[]time.Time | *[]string | *[]float64 | *[]int, got %T", input[0])
		}
		*out = append(*out, in)
	case float64:
		out, ok := output.(*[]float64)
		if !ok {
			return fmt.Errorf("arth: output should be *[]time.Time | *[]string | *[]float64 | *[]int, got %T", input[0])
		}
		*out = append(*out, in)
	default:
		return fmt.Errorf("arth: input should be time.Time | string | float64 | int, got %T", input[0])
	}
	return nil
}

func sumFloat(ans *float64, nn []float64) {
	*ans = 0
	for _, n := range nn {
		*ans += n
	}
}

func sumInt(ans *int, nn []int) {
	*ans = 0
	for _, n := range nn {
		*ans += n
	}
}

func addOne(ans *int, n int) {
	*ans = n + 1
}

func vwap(ans *float64, volumes []int, prices []float64) {
	if len(prices) != len(volumes) {
		panic(fmt.Sprintf("number of volumes and prices must be the same. len(prices): %v len(volumes): %v", len(prices), len(volumes)))
	}
	b := sumints(volumes)
	a := 0.0
	for i := range volumes {
		a += float64(volumes[i]) * prices[i]
	}
	*ans = a / float64(b)
	return
}

func sum(nn []float64) (ans float64) {
	for _, n := range nn {
		ans += n
	}
	return ans
}

func sumints(nn []int) (ans int) {
	for _, n := range nn {
		ans += n
	}
	return ans
}

func copy(output interface{}, input interface{}) error {
	err := fmt.Errorf("can't assign %T to %T", input, output)
	switch d := output.(type) {
	case *time.Time:
		s, ok := input.(time.Time)
		if !ok {
			return err
		}
		*d = s
	case *string:
		s, ok := input.(string)
		if !ok {
			return err
		}
		*d = s
	case *int:
		s, ok := input.(int)
		if !ok {
			return err
		}
		*d = s
	case *float64:
		s, ok := input.(float64)
		if !ok {
			return err
		}
		*d = s
	case *[]time.Time:
		s, ok := input.([]time.Time)
		if !ok {
			return err
		}
		*d = s
	case *[]string:
		s, ok := input.([]string)
		if !ok {
			return err
		}
		*d = s
	case *[]int:
		s, ok := input.([]int)
		if !ok {
			return err
		}
		*d = s
	case *[]float64:
		s, ok := input.([]float64)
		if !ok {
			return err
		}
		*d = s
	default:
		return err
	}
	return nil
}
