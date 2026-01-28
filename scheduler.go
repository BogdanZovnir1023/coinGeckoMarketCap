package main

import (
	"context"
	"database/sql"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
)

type TaskPhase string

const (
	PhaseBackfill    TaskPhase = "backfill"
	PhaseIncremental TaskPhase = "incremental"
)

type Task struct {
	CoinID string
	Symbol string
	From   time.Time
	To     time.Time
	Retry  int
	Phase  TaskPhase
}

type TaskResult struct {
	Task         Task
	Inserted     int
	APIDays      int
	Empty        bool
	HTTPStatus   int
	Err          string
	MissingDates []string
	ActiveNow    bool
}

type CoinState struct {
	SearchEnd   time.Time
	SearchEmpty int
	SeenData    bool

	ConsecutiveEmpty int

	Done bool
}

func makeTaskFixedWindow(coinID, symbol string, end time.Time, startLimit time.Time) (Task, bool) {
	end = dateOnlyUTC(end)
	if end.Before(startLimit) {
		return Task{}, false
	}
	start := end.AddDate(0, 0, -99)
	if start.Before(startLimit) {
		start = startLimit
	}
	return Task{
		CoinID: coinID,
		Symbol: symbol,
		From:   start,
		To:     end,
		Retry:  0,
		Phase:  PhaseBackfill,
	}, true
}

func RunBackfill(ctx context.Context, cfg Config, db *sql.DB, coins []Coin, tasks chan<- Task, results <-chan TaskResult) (map[string]Coin, error) {
	startLimit := cfg.StartDate
	yday := yesterdayUTC()

	states := make(map[string]*CoinState, len(coins))
	active := make(map[string]Coin)

	coinSymbolByID := make(map[string]string, len(coins))
	for _, c := range coins {
		id := strings.TrimSpace(c.ID)
		sym := strings.ToUpper(strings.TrimSpace(c.Symbol))
		if id == "" || sym == "" {
			continue
		}
		coinSymbolByID[id] = sym
		if cfg.CoinIDsFilter != nil && !cfg.CoinIDsFilter[id] {
			continue
		}
		states[id] = &CoinState{SearchEnd: yday}
	}

	total := len(states)
	if total == 0 {
		log.Warn("backfill: no coins to process (after filters)")
		return active, nil
	}

	log.WithFields(log.Fields{
		"coins":         total,
		"start_date":    formatDate(startLimit),
		"yesterday":     formatDate(yday),
		"workers":       cfg.Workers,
		"empty_stop":    cfg.EmptyStopBlocks,
		"max_search":    cfg.MaxSearchBlocks,
		"retry_per_blk": cfg.MaxRetriesPerBlock,
	}).Info("backfill started")

	const progressEvery = 200

	round := 0
	for {

		pending := make([]Task, 0, total)
		doneCount := 0
		scheduledCoins := 0

		for id, st := range states {
			if st.Done {
				doneCount++
				continue
			}

			var end time.Time
			if round == 0 {
				end = yday
			} else {

				if minD, ok, err := getMinDate(ctx, db, cfg.CHTable, id); err == nil && ok {
					end = dateOnlyUTC(minD.AddDate(0, 0, -1))
				} else {

					end = st.SearchEnd
				}
			}

			sym := coinSymbolByID[id]
			if sym == "" {
				sym = strings.ToUpper(id)
			}

			t, ok := makeTaskFixedWindow(id, sym, end, startLimit)
			if !ok {
				st.Done = true
				doneCount++
				continue
			}

			pending = append(pending, t)
			scheduledCoins++
		}

		if doneCount == total {
			break
		}

		log.WithFields(log.Fields{
			"round":          round,
			"scheduledCoins": scheduledCoins,
			"done":           doneCount,
			"total":          total,
			"queue":          len(pending),
		}).Info("backfill round scheduled")

		var (
			inFlight      = 0
			doneTasks     = 0
			sumInserted   = 0
			sumErrors     = 0
			sumEmpty      = 0
			sumRetried    = 0
			sumMissingDay = 0
		)

		for len(pending) > 0 || inFlight > 0 {
			select {
			case <-ctx.Done():
				return active, ctx.Err()

			case res := <-results:
				inFlight--
				doneTasks++

				st := states[res.Task.CoinID]
				if st == nil || st.Done {
					continue
				}

				if len(res.MissingDates) > 0 && res.Task.Retry < cfg.MaxRetriesPerBlock {
					rt := res.Task
					rt.Retry++
					pending = append([]Task{rt}, pending...)
					sumRetried++
					sumMissingDay += len(res.MissingDates)

					log.WithFields(log.Fields{
						"id":      res.Task.CoinID,
						"symbol":  res.Task.Symbol,
						"from":    formatDate(res.Task.From),
						"to":      formatDate(res.Task.To),
						"retry":   rt.Retry,
						"missing": len(res.MissingDates),
						"round":   round,
					}).Warn("block has missing days; retry scheduled")
					continue
				}

				sumInserted += res.Inserted
				if res.Err != "" {
					sumErrors++
					log.WithFields(log.Fields{
						"id":     res.Task.CoinID,
						"symbol": res.Task.Symbol,
						"from":   formatDate(res.Task.From),
						"to":     formatDate(res.Task.To),
						"round":  round,
					}).Warnf("task error: %s", res.Err)
				}

				if res.Empty {
					sumEmpty++
				}

				if res.ActiveNow {
					active[res.Task.CoinID] = Coin{ID: res.Task.CoinID, Symbol: strings.ToLower(res.Task.Symbol)}
				}

				blockHasData := (res.Err == "" && !res.Empty && res.APIDays > 0)

				if blockHasData {
					if !st.SeenData {
						log.WithFields(log.Fields{
							"id":     res.Task.CoinID,
							"symbol": res.Task.Symbol,
							"from":   formatDate(res.Task.From),
							"to":     formatDate(res.Task.To),
							"round":  round,
							"days":   res.APIDays,
						}).Info("first data seen for coin")
					}
					st.SeenData = true
					st.SearchEmpty = 0
					st.ConsecutiveEmpty = 0
				} else {
					if st.SeenData {
						st.ConsecutiveEmpty++
						log.WithFields(log.Fields{
							"id":        res.Task.CoinID,
							"symbol":    res.Task.Symbol,
							"from":      formatDate(res.Task.From),
							"to":        formatDate(res.Task.To),
							"round":     round,
							"empty_seq": st.ConsecutiveEmpty,
						}).Debug("empty block after data (counting towards stop)")
					} else {
						st.SearchEmpty++

						st.SearchEnd = dateOnlyUTC(res.Task.From.AddDate(0, 0, -1))
						log.WithFields(log.Fields{
							"id":          res.Task.CoinID,
							"symbol":      res.Task.Symbol,
							"from":        formatDate(res.Task.From),
							"to":          formatDate(res.Task.To),
							"round":       round,
							"searchEmpty": st.SearchEmpty,
							"nextEnd":     formatDate(st.SearchEnd),
						}).Debug("no data yet; keep searching older")
					}
				}

				if st.SeenData && st.ConsecutiveEmpty >= cfg.EmptyStopBlocks {
					st.Done = true
					log.WithFields(log.Fields{
						"id":     res.Task.CoinID,
						"symbol": res.Task.Symbol,
						"round":  round,
					}).Info("coin backfill completed (empty tail reached)")
				}

				if !st.SeenData && st.SearchEmpty >= cfg.MaxSearchBlocks {
					st.Done = true
					log.WithFields(log.Fields{
						"id":     res.Task.CoinID,
						"symbol": res.Task.Symbol,
						"round":  round,
					}).Warn("coin has no data within search window; stopping")
				}

				if doneTasks%progressEvery == 0 {
					log.WithFields(log.Fields{
						"round":       round,
						"doneTasks":   doneTasks,
						"inFlight":    inFlight,
						"queue":       len(pending),
						"insertedSum": sumInserted,
						"errors":      sumErrors,
						"empty":       sumEmpty,
						"retried":     sumRetried,
						"active":      len(active),
					}).Info("backfill round progress")
				}

			default:

				var outCh chan<- Task
				var next Task
				if len(pending) > 0 {
					outCh = tasks
					next = pending[0]
				}

				select {
				case <-ctx.Done():
					return active, ctx.Err()
				case outCh <- next:
					pending = pending[1:]
					inFlight++
				default:
					time.Sleep(10 * time.Millisecond)
				}
			}
		}

		doneNow := 0
		seenData := 0
		searching := 0
		for _, st := range states {
			if st.Done {
				doneNow++
				continue
			}
			if st.SeenData {
				seenData++
			} else {
				searching++
			}
		}

		log.WithFields(log.Fields{
			"round":          round,
			"doneTasks":      doneTasks,
			"insertedSum":    sumInserted,
			"errors":         sumErrors,
			"empty":          sumEmpty,
			"retried":        sumRetried,
			"missingDaysSum": sumMissingDay,
			"doneCoins":      doneNow,
			"seenDataCoins":  seenData,
			"searchingCoins": searching,
			"activeCoins":    len(active),
		}).Info("backfill round finished")

		round++
	}

	log.WithFields(log.Fields{
		"coins_total":  total,
		"active_coins": len(active),
	}).Info("backfill finished")

	return active, nil
}

func BuildIncrementalTasks(cfg Config, activeCoins []Coin, maxDates map[string]time.Time) []Task {
	yday := yesterdayUTC()
	var tasks []Task

	for _, c := range activeCoins {
		if cfg.CoinIDsFilter != nil && !cfg.CoinIDsFilter[c.ID] {
			continue
		}
		id := strings.TrimSpace(c.ID)
		sym := strings.ToUpper(strings.TrimSpace(c.Symbol))
		if id == "" || sym == "" {
			continue
		}

		maxD, ok := maxDates[id]
		if !ok {
			continue
		}

		start := dateOnlyUTC(maxD.AddDate(0, 0, 1))
		if start.After(yday) {
			continue
		}

		for cur := start; !cur.After(yday); {
			end := cur.AddDate(0, 0, 99)
			if end.After(yday) {
				end = yday
			}
			tasks = append(tasks, Task{
				CoinID: id,
				Symbol: sym,
				From:   cur,
				To:     end,
				Retry:  0,
				Phase:  PhaseIncremental,
			})
			cur = end.AddDate(0, 0, 1)
		}
	}
	return tasks
}
