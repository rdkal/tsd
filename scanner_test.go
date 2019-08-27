package tsd

import (
	"io/ioutil"
	"strings"
	"testing"
)

func Test_Scanner(t *testing.T) {
	rr := ioutil.NopCloser(strings.NewReader(data))
	scanner, err := newMulScanner(10, rr, ioutil.NopCloser(strings.NewReader(data)))
	if err != nil {
		t.Error(err)
	}
	if scanner.Err() != nil {
		t.Error(scanner.Err())
	}
	if !scanner.Scan() {
		t.Error("empty scanner")
	}
	if scanner.Err() != nil {
		t.Error(scanner.Err())
	}
	for scanner.Scan() {
		scanner.Bytes()
	}
	if scanner.Err() != nil {
		t.Error(scanner.Err())
	}
}

func TestScanner_MulFiles(t *testing.T) {

}
