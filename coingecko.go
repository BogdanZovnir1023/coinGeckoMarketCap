package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	log "github.com/sirupsen/logrus"
	"golang.org/x/time/rate"
)

type Coin struct {
	ID     string `json:"id"`
	Symbol string `json:"symbol"`
	Name   string `json:"name"`
}

type MarketChartRangeResp struct {
	Prices       [][]float64 `json:"prices"`
	MarketCaps   [][]float64 `json:"market_caps"`
	TotalVolumes [][]float64 `json:"total_volumes"`
}

type CGClient struct {
	baseURL      string
	apiKey       string
	apiKeyHeader string
	httpClient   *http.Client
	limiter      *rate.Limiter
}

func NewCGClient(cfg Config) *CGClient {
	return &CGClient{
		baseURL:      cfg.CGBaseURL,
		apiKey:       cfg.CGAPIKey,
		apiKeyHeader: cfg.CGAPIKeyHeader,
		httpClient: &http.Client{
			Timeout: cfg.RequestTimeout,
		},
		limiter: rate.NewLimiter(rate.Limit(cfg.CGRPS), cfg.CGBurst),
	}
}

func (c *CGClient) CoinsList(ctx context.Context, status string) ([]Coin, int, []byte, error) {
	q := url.Values{}
	q.Set("include_platform", "false")

	if status != "" && status != "active" {
		q.Set("status", status) //
	}

	full := c.baseURL + "/coins/list?" + q.Encode()

	statusCode, body, err := c.getJSONRaw(ctx, full)
	if err != nil {
		return nil, statusCode, body, err
	}

	var coins []Coin
	if err := json.Unmarshal(body, &coins); err != nil {
		return nil, statusCode, body, err
	}
	return coins, statusCode, body, nil
}

func (c *CGClient) MarketChartRange(ctx context.Context, id, vs, fromDate, toDate, interval string) (MarketChartRangeResp, int, []byte, error) {
	q := url.Values{}
	q.Set("vs_currency", vs)
	q.Set("from", fromDate)
	q.Set("to", toDate)
	if interval != "" {
		q.Set("interval", interval)
	}
	full := fmt.Sprintf("%s/coins/%s/market_chart/range?%s", c.baseURL, url.PathEscape(id), q.Encode())

	status, body, err := c.getJSONRaw(ctx, full)
	if err != nil {
		return MarketChartRangeResp{}, status, body, err
	}

	var out MarketChartRangeResp
	if err := json.Unmarshal(body, &out); err != nil {
		return MarketChartRangeResp{}, status, body, err
	}
	return out, status, body, nil
}

func (c *CGClient) getJSONRaw(ctx context.Context, fullURL string) (int, []byte, error) {
	if err := c.limiter.Wait(ctx); err != nil {
		return 0, nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fullURL, nil)
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "cg-range-etl/1.0")
	if c.apiKey != "" {
		req.Header.Set(c.apiKeyHeader, c.apiKey)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode >= 400 {
		return resp.StatusCode, body, fmt.Errorf("http %d: %s", resp.StatusCode, truncate(body, 500))
	}

	return resp.StatusCode, body, nil
}

func truncate(b []byte, n int) string {
	s := string(b)
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

func isRetryableStatus(code int) bool {
	if code == 0 {
		return true
	}
	switch code {
	case 408, 425, 429, 500, 502, 503, 504:
		return true
	default:
		return false
	}
}

func backoffSleep(attempt int) time.Duration {
	d := time.Second * time.Duration(1<<attempt)
	if d > 30*time.Second {
		d = 30 * time.Second
	}
	return d
}

func logHTTPError(coinID string, from, to string, status int, body []byte, err error) {
	log.WithFields(log.Fields{
		"id":     coinID,
		"from":   from,
		"to":     to,
		"status": status,
	}).Warnf("coingecko error: %v; body=%s", err, truncate(body, 300))
}
