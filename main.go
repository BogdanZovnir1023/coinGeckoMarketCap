package main

import (
	"context"
	"database/sql"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"time"

	log "github.com/sirupsen/logrus"
)

func main() {
	cfg := LoadConfig()

	lvl, err := log.ParseLevel(cfg.LogLevel)
	if err != nil {
		lvl = log.InfoLevel
	}
	log.SetLevel(lvl)
	log.SetFormatter(&log.TextFormatter{FullTimestamp: true})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		ch := make(chan os.Signal, 2)
		signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
		<-ch
		log.Warn("shutdown signal received")
		cancel()
	}()

	cg := NewCGClient(cfg)

	db, err := openClickHouse(ctx, cfg)
	if err != nil {
		log.Fatalf("clickhouse connect: %v", err)
	}
	defer db.Close()

	if err := createTable(ctx, db, cfg.CHTable); err != nil {
		log.Fatalf("create table: %v", err)
	}

	allCoins, activeCoinsAPI := fetchCoinsLists(ctx, cg, cfg)
	log.WithFields(log.Fields{
		"coins_total":  len(allCoins),
		"active_total": len(activeCoinsAPI),
	}).Info("coins list loaded")

	tasksCh := make(chan Task, cfg.Workers*2)
	resultsCh := make(chan TaskResult, cfg.Workers*4)

	for i := 0; i < cfg.Workers; i++ {
		go worker(ctx, i, cfg, cg, db, tasksCh, resultsCh)
	}

	activeDetected, err := RunBackfill(ctx, cfg, db, allCoins, tasksCh, resultsCh)
	if err != nil {
		log.Fatalf("backfill failed: %v", err)
	}

	activeCoins := activeCoinsAPI
	if len(activeCoins) == 0 {
		log.Warn("active coins list from API is empty; fallback to backfill-detected active coins")
		activeCoins = coinFromIDMap(activeDetected)
	}

	log.WithField("active_coins_for_incremental", len(activeCoins)).Info("incremental target set")

	ticker := time.NewTicker(cfg.SyncEvery)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Info("exit")
			return
		default:
		}

		runIncrementalOnce(ctx, cfg, db, activeCoins, tasksCh, resultsCh)

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func fetchCoinsLists(ctx context.Context, cg *CGClient, cfg Config) (all []Coin, active []Coin) {
	act, stA, bA, err := cg.CoinsList(ctx, "")
	if err != nil {
		log.WithField("status", stA).Warnf("coins/list active(default) failed: %v; body=%s", err, truncate(bA, 300))
		act = nil
	}

	inact, stI, bI, err := cg.CoinsList(ctx, "inactive")
	if err != nil {
		log.WithField("status", stI).Warnf("coins/list inactive failed: %v; body=%s", err, truncate(bI, 300))
		inact = nil
	}

	activeByID := make(map[string]Coin, len(act))
	for _, c := range act {
		c.ID = strings.TrimSpace(c.ID)
		c.Symbol = strings.TrimSpace(c.Symbol)
		if c.ID == "" {
			continue
		}
		if cfg.CoinIDsFilter != nil && !cfg.CoinIDsFilter[c.ID] {
			continue
		}
		activeByID[c.ID] = c
	}

	seen := make(map[string]Coin, len(act)+len(inact))
	for _, c := range act {
		c.ID = strings.TrimSpace(c.ID)
		c.Symbol = strings.TrimSpace(c.Symbol)
		if c.ID == "" {
			continue
		}
		if cfg.CoinIDsFilter != nil && !cfg.CoinIDsFilter[c.ID] {
			continue
		}
		if _, ok := seen[c.ID]; !ok {
			seen[c.ID] = c
		}
	}
	for _, c := range inact {
		c.ID = strings.TrimSpace(c.ID)
		c.Symbol = strings.TrimSpace(c.Symbol)
		if c.ID == "" {
			continue
		}
		if cfg.CoinIDsFilter != nil && !cfg.CoinIDsFilter[c.ID] {
			continue
		}
		if _, ok := seen[c.ID]; !ok {
			seen[c.ID] = c
		}
	}

	all = make([]Coin, 0, len(seen))
	for _, c := range seen {
		all = append(all, c)
	}

	active = make([]Coin, 0, len(activeByID))
	for _, c := range activeByID {
		active = append(active, c)
	}

	return all, active
}

func runIncrementalOnce(
	ctx context.Context,
	cfg Config,
	db *sql.DB,
	activeCoins []Coin,
	tasksCh chan<- Task,
	resultsCh <-chan TaskResult,
) {
	if len(activeCoins) == 0 {
		log.Warn("incremental: no active coins")
		return
	}

	maxDates := make(map[string]time.Time, len(activeCoins))
	for _, c := range activeCoins {
		id := strings.TrimSpace(c.ID)
		if id == "" {
			continue
		}
		md, ok, err := getMaxDate(ctx, db, cfg.CHTable, id)
		if err != nil {
			log.WithField("id", id).Warnf("max date query failed: %v", err)
			continue
		}
		if ok {
			maxDates[id] = md
		}
	}

	tasks := BuildIncrementalTasks(cfg, activeCoins, maxDates)
	if len(tasks) == 0 {
		log.Info("incremental: nothing to do")
		return
	}

	log.WithFields(log.Fields{
		"tasks":  len(tasks),
		"active": len(activeCoins),
	}).Info("incremental started")

	pending := append([]Task{}, tasks...)
	inFlight := 0

	for len(pending) > 0 || inFlight > 0 {
		select {
		case <-ctx.Done():
			return

		case res := <-resultsCh:
			inFlight--

			if res.Err != "" {
				log.WithFields(log.Fields{
					"id":     res.Task.CoinID,
					"symbol": res.Task.Symbol,
					"from":   formatDate(res.Task.From),
					"to":     formatDate(res.Task.To),
					"retry":  res.Task.Retry,
				}).Warnf("incremental task error (no requeue here): %s", res.Err)
				continue
			}

			if len(res.MissingDates) > 0 && res.Task.Retry < cfg.MaxRetriesPerBlock {
				rt := res.Task
				rt.Retry++
				pending = append([]Task{rt}, pending...)
				continue
			}

		default:
			var outCh chan<- Task
			var next Task
			if len(pending) > 0 {
				outCh = tasksCh
				next = pending[0]
			}

			select {
			case <-ctx.Done():
				return
			case outCh <- next:
				pending = pending[1:]
				inFlight++
			default:
				time.Sleep(10 * time.Millisecond)
			}
		}
	}

	log.Info("incremental finished")
}

func coinFromIDMap(m map[string]Coin) []Coin {
	out := make([]Coin, 0, len(m))
	for id, c := range m {
		if c.ID == "" {
			c.ID = id
		}
		out = append(out, c)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}
