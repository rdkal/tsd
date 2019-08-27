package tsd

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type series struct {
	Root   string
	series string
	Fields map[string]fieldInfo
	Kinds  []string
	Names  []string

	// same length
	Files []string
	Dates []time.Time

	Glob       string
	DateLayout string
	Index      string
}

func newSeries(relativePath string) (*series, error) {
	var ok bool
	s := &series{
		series: relativePath,
	}
	s.Root, ok = os.LookupEnv("TSDPATH")
	if !ok {
		return nil, fmt.Errorf("set enironment variable TSDPATH to the path of the data folder")
	}
	if err := s.readConfig(); err != nil {
		return nil, err
	}
	var err error
	s.Files, err = s.resourcePaths()
	if err != nil {
		return nil, err
	}
	if err := s.dates(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *series) Exec(q Query) Iter {
	return &records{}
}

func (s *series) readConfig() error {
	type config struct {
		FilesPattern   string      `json:"files-pattern"`
		FileDateLayout string      `json:"file-date-layout"`
		TimeIndexField string      `json:"time-index-field"`
		Types          []fieldInfo `json:"types"`
	}

	path := filepath.Join(s.Root, s.series, "config.json")
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("could not find config at %v", path)
	}
	defer file.Close()
	var cfg config
	if err := json.NewDecoder(file).Decode(&cfg); err != nil {
		return err
	}
	s.DateLayout = cfg.FileDateLayout
	s.Fields = make(map[string]fieldInfo)
	for i, info := range cfg.Types {
		info.Pos = Input(i)
		s.Fields[info.Field] = info
	}
	s.Index = cfg.TimeIndexField
	s.Glob = cfg.FilesPattern
	for _, info := range cfg.Types {
		s.Kinds = append(s.Kinds, info.Kind)
		s.Names = append(s.Names, info.Field)
	}
	return nil
}

func (s *series) resourcePaths() ([]string, error) {
	fullPath := filepath.Join(s.Root, s.series, s.Glob)
	matches, err := filepath.Glob(fullPath)
	if err != nil {
		return nil, err
	}
	return matches, nil
}

func (s *series) dates() error {
	s.Dates = make([]time.Time, len(s.Files))
	for i, path := range s.Files {
		t, err := s.parseDate(path)
		if err != nil {
			return err
		}
		s.Dates[i] = t
	}
	return nil
}

func (s *series) parseDate(path string) (time.Time, error) {
	var date time.Time
	var err error
	filename := filepath.Base(path)
	for _, field := range strings.Split(filename, ".") {
		date, err = time.Parse(s.DateLayout, field)
		if err != nil {
			continue
		}
		break
	}
	if date.IsZero() {
		return date, fmt.Errorf("could not find %v in %v", s.DateLayout, path)
	}
	return date, nil
}

type fieldInfo struct {
	Field  string `json:"field"`
	Kind   string `json:"kind"`
	Layout string `json:"layout"`
	Pos    Input
}

type resource struct {
	path string
	date time.Time
}
