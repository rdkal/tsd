TSD
=====

TSD queires time-series data in CSV files.
Environment varible TSDPATH should be set to root of data.

```go
q := tsd.Query{
  Series: "series/weather-data",
  Where:  []string{"kind == RAIN"},
  Project: []tsd.Projection{
    {
      F:  arth.Sum,
      Of: []string{"volume"},
    },
  },
  From:   time.Date(2019, 05, 01, 00, 00, 0, 0, time.UTC),
  To:     time.Date(2019, 8, 01, 00, 00, 0, 0, time.UTC),
  Period: 1 * time.Hour,
}
iter := tsd.Exec(q)
if iter.Err() != nil {
  log.Fatal(iter.Err())
}
var (
  sum int
  t time.Time
)
for iter.Next() {
  if err := iter.Scan(&sum); err != nil {
    log.Println(err)
  }
  t = iter.Cursor()
}
```
