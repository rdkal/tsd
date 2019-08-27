package tsd

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
)

type mulScanner struct {
	i          int
	fieldCount int
	rr         []io.ReadCloser
	s          *bufio.Scanner

	err error
}

func newMulScanner(fieldCount int, rr ...io.ReadCloser) (*mulScanner, error) {
	s := bufio.NewScanner(rr[0])
	if s.Scan() {
		got := len(bytes.Split(s.Bytes(), fieldSep))
		if fieldCount != got {
			return nil, fmt.Errorf("field count should be %v got %v", fieldCount, got)
		}
	}
	return &mulScanner{
		i:          0,
		fieldCount: fieldCount,
		rr:         rr,
		s:          s,
	}, nil
}

func (ms *mulScanner) Scan() bool {
	if ms.err != nil {
		return false
	}
	if !(ms.i < len(ms.rr)) {
		return false
	}
	if !ms.s.Scan() {
		return ms.scan()
	}
	return true
}

// scan goes to the next scan scanner and skips first line
func (ms *mulScanner) scan() bool {
	ms.i++
	if !(ms.i < len(ms.rr)) {
		return false
	}
	ms.s = bufio.NewScanner(ms.rr[ms.i])
	if !ms.s.Scan() {
		return ms.scan()
	} else {
		header := bytes.Split(ms.s.Bytes(), fieldSep)
		length := len(header)
		if length != ms.fieldCount {
			fmt.Println(header)
			ms.err = fmt.Errorf("field count should be %v got %v", ms.fieldCount, length)
			return false
		}
		return ms.s.Scan()
	}
}

func (ms *mulScanner) Bytes() []byte {
	return ms.s.Bytes()
}

func (ms *mulScanner) Err() error {
	return ms.err
}

func (ms *mulScanner) Close() error {
	var err error
	for _, r := range ms.rr {
		if r == nil {
			continue
		}
		_err := r.Close()
		if _err != nil {
			if err != nil {
				err = _err
			}
		}
	}
	if ms.err == nil {
		ms.err = err
	}
	return err
}
