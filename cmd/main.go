package main

import (
	"bufio"
	"fmt"
	"kal/tsd"
	"kal/tsd/arth"
	"log"
	"os"
	"runtime"
	"strconv"
	"time"
)

func main() {
	// defer profile.Start().Stop()
	file, err := os.Create("data.csv")
	if err != nil {
		log.Fatal(err)
	}
	w := bufio.NewWriter(file)
	q := tsd.Query{
		Series: "bitmex/trade",
		Where:  []string{"symbol == XBTUSD"},
		Project: []tsd.Projection{
			{
				F:  arth.VWAP,
				Of: []string{"size", "price"},
			},
		},
		From:   time.Date(2019, 05, 01, 00, 00, 0, 0, time.UTC),
		To:     time.Date(2019, 05, 02, 00, 00, 0, 0, time.UTC),
		Period: 1 * time.Minute,
	}
	iter := tsd.Exec(q)
	var vwap float64
	for iter.Next() {
		iter.Scan(&vwap)
		w.WriteString(strconv.FormatFloat(vwap, 'e', -1, 64))
		w.WriteByte('\n')
	}
	if iter.Err() != nil {
		log.Fatal(iter.Err())
	}
	if err := w.Flush(); err != nil {
		log.Fatal(err)
	}
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	fmt.Println("Total allocs:", stats.TotalAlloc)

}
