package main

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"strings"
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
	Timestamp  time.Time // UTC
	Price      float64
	MarketCap  float64
	Volume     float64
}

func chDSN(host, port, user, pass, db string) string {
	u := &url.URL{
		Scheme: "clickhouse",
		User:   url.UserPassword(user, pass),
		Host:   fmt.Sprintf("%s:%s", host, port),
		Path:   "/" + db,
	}
	q := url.Values{}
	q.Set("dial_timeout", "10s")
	q.Set("read_timeout", "5m")
	u.RawQuery = q.Encode()
	return u.String()
}

func openClickHouse(ctx context.Context, cfg Config) (*sql.DB, error) {
	admin, err := sql.Open("clickhouse", chDSN(cfg.CHHost, cfg.CHPort, cfg.CHUser, cfg.CHPassword, "default"))
	if err != nil {
		return nil, err
	}
	admin.SetMaxOpenConns(4)
	admin.SetMaxIdleConns(4)
	admin.SetConnMaxLifetime(30 * time.Minute)

	if _, err := admin.ExecContext(ctx, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", cfg.CHDatabase)); err != nil {
		_ = admin.Close()
		return nil, err
	}
	_ = admin.Close()

	db, err := sql.Open("clickhouse", chDSN(cfg.CHHost, cfg.CHPort, cfg.CHUser, cfg.CHPassword, cfg.CHDatabase))
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(16)
	db.SetMaxIdleConns(16)
	db.SetConnMaxLifetime(30 * time.Minute)

	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

func createTable(ctx context.Context, db *sql.DB, table string) error {
	_, err := db.ExecContext(ctx, fmt.Sprintf(createCoinGeckoTable, table))
	return err
}

func getExistingDays(ctx context.Context, db *sql.DB, table, id string, from, to time.Time) (map[string]struct{}, error) {
	q := fmt.Sprintf(`
SELECT toString(_date) as d
FROM %s
WHERE id = ? AND _date BETWEEN toDate(?) AND toDate(?)
GROUP BY d`, table)

	rows, err := db.QueryContext(ctx, q, id, formatDate(from), formatDate(to))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make(map[string]struct{})
	for rows.Next() {
		var d string
		if err := rows.Scan(&d); err != nil {
			return nil, err
		}
		out[d] = struct{}{}
	}
	return out, rows.Err()
}

func getMaxDate(ctx context.Context, db *sql.DB, table, id string) (time.Time, bool, error) {
	q := fmt.Sprintf(`SELECT max(_date) FROM %s WHERE id = ?`, table)
	var dt sql.NullTime
	if err := db.QueryRowContext(ctx, q, id).Scan(&dt); err != nil {
		return time.Time{}, false, err
	}
	if !dt.Valid {
		return time.Time{}, false, nil
	}
	return dateOnlyUTC(dt.Time), true, nil
}

func getMinDate(ctx context.Context, db *sql.DB, table, id string) (time.Time, bool, error) {
	q := fmt.Sprintf(`SELECT min(_date) FROM %s WHERE id = ?`, table)
	var dt sql.NullTime
	if err := db.QueryRowContext(ctx, q, id).Scan(&dt); err != nil {
		return time.Time{}, false, err
	}
	if !dt.Valid {
		return time.Time{}, false, nil
	}
	return dateOnlyUTC(dt.Time), true, nil
}

func insertDailyPoints(ctx context.Context, db *sql.DB, table string, pts []DailyPoint) (int, error) {
	if len(pts) == 0 {
		return 0, nil
	}

	var sb strings.Builder
	sb.WriteString("INSERT INTO ")
	sb.WriteString(table)
	sb.WriteString(" (id, symbol, vs_currency, timestamp, price, market_cap, volume) VALUES ")

	args := make([]any, 0, len(pts)*7)
	for i, p := range pts {
		if i > 0 {
			sb.WriteString(",")
		}
		sb.WriteString("(?, ?, ?, ?, ?, ?, ?)")
		args = append(args,
			p.ID,
			p.Symbol,
			p.VsCurrency,
			p.Timestamp.UTC(),
			p.Price,
			p.MarketCap,
			p.Volume,
		)
	}

	_, err := db.ExecContext(ctx, sb.String(), args...)
	if err != nil {
		return 0, err
	}
	return len(pts), nil
}
