package main

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"time"

	"github.com/sirupsen/logrus"
)

type dailyAgg struct {
	ts time.Time
	p  float64
	mc float64
	v  float64
}

func worker(ctx context.Context, wid int, cfg Config, cg *CGClient, db *sql.DB, tasks <-chan Task, results chan<- TaskResult) {
	for {
		select {
		case <-ctx.Done():
			return
		case t, ok := <-tasks:
			if !ok {
				return
			}
			res := handleTask(ctx, cfg, cg, db, t)
			results <- res
		}
	}
}

func handleTask(ctx context.Context, cfg Config, cg *CGClient, db *sql.DB, t Task) TaskResult {
	fromStr := formatDate(t.From)
	toStr := formatDate(t.To)

	allDays := daysInclusive(t.From, t.To)

	existing, err := getExistingDays(ctx, db, cfg.CHTable, t.CoinID, t.From, t.To)
	if err == nil && t.Retry == 0 && len(existing) == len(allDays) && len(allDays) > 0 {
		return TaskResult{
			Task:       t,
			Inserted:   0,
			APIDays:    0,
			Empty:      false,
			HTTPStatus: 200,
			Err:        "",
		}
	}

	var resp MarketChartRangeResp
	var status int
	var lastBody []byte
	var lastErr error

	for attempt := 0; attempt <= cfg.MaxRetriesPerBlock; attempt++ {
		r, st, b, e := cg.MarketChartRange(ctx, t.CoinID, cfg.VsCurrency, fromStr, toStr, cfg.Interval)
		resp, status, lastBody, lastErr = r, st, b, e
		if e == nil {
			break
		}
		if !isRetryableStatus(st) {
			break
		}
		logHTTPError(t.CoinID, fromStr, toStr, st, b, e)
		time.Sleep(backoffSleep(attempt))
	}

	if lastErr != nil {
		return TaskResult{
			Task:       t,
			Inserted:   0,
			APIDays:    0,
			Empty:      true,
			HTTPStatus: status,
			Err:        fmt.Sprintf("%v; body=%s", lastErr, truncate(lastBody, 300)),
		}
	}

	byDay := make(map[string]*dailyAgg)

	apply := func(arr [][]float64, kind string) {
		for _, row := range arr {
			if len(row) < 2 {
				continue
			}
			ms := int64(row[0])
			val := row[1]
			ts := time.UnixMilli(ms).UTC()
			day := ts.Format("2006-01-02")

			a := byDay[day]
			if a == nil {
				a = &dailyAgg{ts: ts}
				byDay[day] = a
			}
			if ts.After(a.ts) {
				a.ts = ts
			}
			switch kind {
			case "p":
				a.p = val
			case "mc":
				a.mc = val
			case "v":
				a.v = val
			}
		}
	}

	apply(resp.Prices, "p")
	apply(resp.MarketCaps, "mc")
	apply(resp.TotalVolumes, "v")

	if len(byDay) == 0 {
		return TaskResult{
			Task:       t,
			Inserted:   0,
			APIDays:    0,
			Empty:      true,
			HTTPStatus: 200,
			Err:        "",
		}
	}

	apiDays := make([]string, 0, len(byDay))
	for d := range byDay {
		apiDays = append(apiDays, d)
	}
	sort.Strings(apiDays)

	if existing == nil {
		existing, _ = getExistingDays(ctx, db, cfg.CHTable, t.CoinID, t.From, t.To)
	}

	toInsert := make([]DailyPoint, 0, len(apiDays))
	for _, day := range apiDays {
		if _, ok := existing[day]; ok {
			continue
		}
		a := byDay[day]
		toInsert = append(toInsert, DailyPoint{
			ID:         t.CoinID,
			Symbol:     t.Symbol,
			VsCurrency: cfg.VsCurrency,
			Timestamp:  a.ts,
			Price:      a.p,
			MarketCap:  a.mc,
			Volume:     a.v,
		})
	}

	inserted, insErr := insertDailyPoints(ctx, db, cfg.CHTable, toInsert)
	if insErr != nil {
		return TaskResult{
			Task:       t,
			Inserted:   0,
			APIDays:    len(apiDays),
			Empty:      false,
			HTTPStatus: 200,
			Err:        insErr.Error(),
		}
	}

	after, err2 := getExistingDays(ctx, db, cfg.CHTable, t.CoinID, t.From, t.To)
	missing := make([]string, 0)
	if err2 == nil {
		for _, d := range apiDays {
			if _, ok := after[d]; !ok {
				missing = append(missing, d)
			}
		}
		sort.Strings(missing)
	}

	activeNow := false
	yday := yesterdayUTC()
	if t.To.Equal(yday) || t.To.After(yday.AddDate(0, 0, -2)) {
		maxDay := apiDays[len(apiDays)-1]
		if maxDay >= formatDate(yday.AddDate(0, 0, -2)) {
			activeNow = true
		}
	}

	if len(missing) > 0 {
		WithFields(Fields{
			"id":      t.CoinID,
			"symbol":  t.Symbol,
			"from":    formatDate(t.From),
			"to":      formatDate(t.To),
			"missing": len(missing),
		}).Warn("api-days missing in DB after insert")
	}

	return TaskResult{
		Task:         t,
		Inserted:     inserted,
		APIDays:      len(apiDays),
		Empty:        false,
		HTTPStatus:   200,
		Err:          "",
		MissingDates: missing,
		ActiveNow:    activeNow,
	}
}
