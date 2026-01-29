package main

import (
	"os"
	"strconv"
	"time"
)

type Config struct {
	CGBaseURL      string
	CGAPIKey       string
	CGAPIKeyHeader string
	VsCurrency     string
	Interval       string
	RequestTimeout time.Duration
	CGRPS          float64
	CGBurst        int
	CoinIDsFilter  map[string]bool

	CHHost     string
	CHPort     string
	CHUser     string
	CHPassword string
	CHDatabase string
	CHTable    string

	Workers            int
	StartDate          time.Time
	EmptyStopBlocks    int
	MaxSearchBlocks    int
	MaxRetriesPerBlock int
	SyncEvery          time.Duration
	LogLevel           string
}

func LoadConfig() Config {
	cfg := Config{
		CGBaseURL:      getenv("COINGECKO_BASE_URL", "https://pro-api.coingecko.com/api/v3"),
		CGAPIKey:       getenv("COINGECKO_API_KEY", ""),
		CGAPIKeyHeader: getenv("COINGECKO_API_KEY_HEADER", "x-cg-pro-api-key"),
		VsCurrency:     getenv("COINGECKO_VS_CURRENCY", "usd"),
		Interval:       getenv("COINGECKO_INTERVAL", "daily"),
		RequestTimeout: mustDuration(getenv("COINGECKO_TIMEOUT", "30s")),
		CGRPS:          mustFloat(getenv("COINGECKO_RPS", "6")),  // подстрой под свой план.
		CGBurst:        mustInt(getenv("COINGECKO_BURST", "12")), // подстрой под свой план

		CHHost:     getenv("CLICKHOUSE_HOST", "localhost"),
		CHPort:     getenv("CLICKHOUSE_PORT", "9000"),
		CHUser:     getenv("CLICKHOUSE_USER", "clickhouse"),
		CHPassword: getenv("CLICKHOUSE_PASSWORD", "clickhouse"),
		CHDatabase: getenv("CLICKHOUSE_DATABASE", "default"),
		CHTable:    getenv("CLICKHOUSE_TABLE", "coingecko_market_cap_daily"),

		Workers:            mustInt(getenv("WORKERS", "8")),
		EmptyStopBlocks:    mustInt(getenv("EMPTY_STOP_BLOCKS", "2")),
		MaxSearchBlocks:    mustInt(getenv("MAX_SEARCH_BLOCKS", "30")), // NEW
		MaxRetriesPerBlock: mustInt(getenv("MAX_RETRIES_PER_BLOCK", "3")),
		SyncEvery:          mustDuration(getenv("SYNC_EVERY", "6h")),
		LogLevel:           getenv("LOG_LEVEL", "info"),
	}

	cfg.StartDate = mustParseDate(getenv("START_DATE", "2018-01-01"))

	cfg.CoinIDsFilter = parseCSVSet(os.Getenv("COINGECKO_IDS"))

	return cfg
}

func getenv(k, def string) string {
	v := os.Getenv(k)
	if v == "" {
		return def
	}
	return v
}

func mustInt(s string) int {
	n, err := strconv.Atoi(s)
	if err != nil {
		panic("bad int: " + s)
	}
	return n
}

func mustFloat(s string) float64 {
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		panic("bad float: " + s)
	}
	return f
}

func mustDuration(s string) time.Duration {
	d, err := time.ParseDuration(s)
	if err != nil {
		panic("bad duration: " + s)
	}
	return d
}
