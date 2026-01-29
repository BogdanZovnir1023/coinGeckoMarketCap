package main

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"time"

	_ "github.com/ClickHouse/clickhouse-go/v2"
)

const createCoinGeckoTable = `
CREATE TABLE IF NOT EXISTS %s
(
    _date       Date DEFAULT toDate(timestamp),
    id          LowCardinality(String),
    symbol      LowCardinality(String),
    vs_currency LowCardinality(String),
    timestamp   DateTime64(3, 'UTC'),
    price       Float64,
    market_cap  Float64,
    volume      Float64
) ENGINE = MergeTree
PARTITION BY toYYYYMM(_date)
ORDER BY (_date, id, symbol, vs_currency, timestamp)
SETTINGS index_granularity = 8192;
`

type DailyPoint struct {
	ID         string
	Symbol     string
	VsCurrency string
	Timestamp  time.Time
	Price      float64
	MarketCap  float64
	Volume     float64
}

func openClickHouse(ctx context.Context, cfg Config) (*sql.DB, error) {
	// connect to default DB to create target DB (if needed)
	admin, err := sql.Open("clickhouse", clickhouseDSN(cfg, "default"))
	if err != nil {
		return nil, err
	}
	admin.SetMaxOpenConns(10)
	admin.SetMaxIdleConns(10)
	admin.SetConnMaxLifetime(30 * time.Minute)

	if err := admin.PingContext(ctx); err != nil {
		_ = admin.Close()
		return nil, err
	}

	if cfg.CHDatabase != "" && cfg.CHDatabase != "default" {
		_, err = admin.ExecContext(ctx, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", cfg.CHDatabase))
		if err != nil {
			_ = admin.Close()
			return nil, err
		}
	}
	_ = admin.Close()

	dbName := cfg.CHDatabase
	if dbName == "" {
		dbName = "default"
	}

	db, err := sql.Open("clickhouse", clickhouseDSN(cfg, dbName))
	if err != nil {
		return nil, err
	}

	db.SetMaxOpenConns(cfg.Workers + 5)
	db.SetMaxIdleConns(cfg.Workers + 5)
	db.SetConnMaxLifetime(30 * time.Minute)

	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}

	return db, nil
}

func clickhouseDSN(cfg Config, database string) string {
	host := cfg.CHHost
	if host == "" {
		host = "localhost"
	}
	port := cfg.CHPort
	if port == "" {
		port = "9000"
	}
	user := cfg.CHUser
	if user == "" {
		user = "default"
	}

	u := &url.URL{
		Scheme: "clickhouse",
		User:   url.UserPassword(user, cfg.CHPassword),
		Host:   fmt.Sprintf("%s:%s", host, port),
		Path:   "/" + database,
	}

	q := u.Query()
	q.Set("compress", "lz4")
	q.Set("dial_timeout", "10s")
	q.Set("read_timeout", "30s")
	u.RawQuery = q.Encode()

	return u.String()
}

func createTable(ctx context.Context, db *sql.DB, table string) error {
	_, err := db.ExecContext(ctx, fmt.Sprintf(createCoinGeckoTable, table))
	return err
}

func getMaxDate(ctx context.Context, db *sql.DB, table, coinID string) (time.Time, bool, error) {
	var nt sql.NullTime
	q := fmt.Sprintf("SELECT maxOrNull(_date) FROM %s WHERE id = ?", table)
	if err := db.QueryRowContext(ctx, q, coinID).Scan(&nt); err != nil {
		return time.Time{}, false, err
	}
	if !nt.Valid {
		return time.Time{}, false, nil
	}
	t := time.Date(nt.Time.Year(), nt.Time.Month(), nt.Time.Day(), 0, 0, 0, 0, time.UTC)
	return t, true, nil
}

func getMinDate(ctx context.Context, db *sql.DB, table, coinID string) (time.Time, bool, error) {
	var nt sql.NullTime
	q := fmt.Sprintf("SELECT minOrNull(_date) FROM %s WHERE id = ?", table)
	if err := db.QueryRowContext(ctx, q, coinID).Scan(&nt); err != nil {
		return time.Time{}, false, err
	}
	if !nt.Valid {
		return time.Time{}, false, nil
	}
	t := time.Date(nt.Time.Year(), nt.Time.Month(), nt.Time.Day(), 0, 0, 0, 0, time.UTC)
	return t, true, nil
}

func getExistingDays(ctx context.Context, db *sql.DB, table, coinID string, from, to time.Time) (map[string]struct{}, error) {
	m := make(map[string]struct{})

	q := fmt.Sprintf(`
SELECT toString(_date)
FROM %s
WHERE id = ?
  AND _date >= toDate(?)
  AND _date <= toDate(?)
GROUP BY _date
`, table)

	rows, err := db.QueryContext(ctx, q, coinID, from, to)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var d string
		if err := rows.Scan(&d); err != nil {
			return nil, err
		}
		m[d] = struct{}{}
	}
	return m, rows.Err()
}

func insertDailyPoints(ctx context.Context, db *sql.DB, table string, pts []DailyPoint) (int, error) {
	if len(pts) == 0 {
		return 0, nil
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback() }()

	stmt, err := tx.PrepareContext(ctx,
		fmt.Sprintf("INSERT INTO %s (id, symbol, vs_currency, timestamp, price, market_cap, volume) VALUES (?, ?, ?, ?, ?, ?, ?)", table),
	)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()

	for _, p := range pts {
		_, err := stmt.ExecContext(ctx, p.ID, p.Symbol, p.VsCurrency, p.Timestamp, p.Price, p.MarketCap, p.Volume)
		if err != nil {
			return 0, err
		}
	}

	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return len(pts), nil
}
