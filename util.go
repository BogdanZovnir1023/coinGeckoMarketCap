package main

import (
	"strings"
	"time"
)

func mustParseDate(s string) time.Time {
	t, err := time.ParseInLocation("2006-01-02", s, time.UTC)
	if err != nil {
		panic("bad date: " + s)
	}
	return dateOnlyUTC(t)
}

func dateOnlyUTC(t time.Time) time.Time {
	u := t.UTC()
	return time.Date(u.Year(), u.Month(), u.Day(), 0, 0, 0, 0, time.UTC)
}

func yesterdayUTC() time.Time {
	return dateOnlyUTC(time.Now().UTC().AddDate(0, 0, -1))
}

func formatDate(t time.Time) string {
	return dateOnlyUTC(t).Format("2006-01-02")
}

func daysInclusive(from, to time.Time) []time.Time {
	f := dateOnlyUTC(from)
	t := dateOnlyUTC(to)
	if t.Before(f) {
		return nil
	}
	var out []time.Time
	for d := f; !d.After(t); d = d.AddDate(0, 0, 1) {
		out = append(out, d)
	}
	return out
}

func parseCSVSet(s string) map[string]bool {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	set := make(map[string]bool)
	parts := strings.Split(s, ",")
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		set[p] = true
	}
	return set
}
