// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package main demonstrates a user activity sessionization pipeline
// using Apache Beam with PostgreSQL.
//
// Spark Equivalent:
//
//	df.groupBy(session_window(col("timestamp"), "30 minutes"), col("user_id"))
//	  .agg(count("*").as("event_count"), min("timestamp").as("session_start"), max("timestamp").as("session_end"))
//	  .write.format("jdbc").mode("overwrite").save()
//
// Use Case:
// Web and mobile analytics aggregate clickstream event logs into continuous user
// sessions, delimiting new sessions whenever user inactivity exceeds a duration
// threshold (e.g. 30 minutes).
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"reflect"
	"sort"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/postgresio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
)

var (
	host     = flag.String("host", "localhost", "PostgreSQL host")
	port     = flag.Int("port", 5432, "PostgreSQL port")
	database = flag.String("database", "postgres", "PostgreSQL database name")
	username = flag.String("username", "beam_test", "PostgreSQL user")
	password = flag.String("password", "beam_test", "PostgreSQL password")
	gapMins  = flag.Int("gap_minutes", 30, "Inactivity threshold in minutes to delimit new sessions")
)

func init() {
	beam.RegisterType(reflect.TypeOf((*ClickEvent)(nil)).Elem())
	beam.RegisterType(reflect.TypeOf((*UserSessionSummary)(nil)).Elem())
	beam.RegisterDoFn(&keyByUserFn{})
	beam.RegisterDoFn(&sessionizeUserClicksFn{})
}

// ClickEvent represents an incoming user clickstream event.
type ClickEvent struct {
	UserID    string    `json:"user_id" db:"user_id" beam:"user_id"`
	EventType string    `json:"event_type" db:"event_type" beam:"event_type"`
	URL       string    `json:"url" db:"url" beam:"url"`
	Timestamp time.Time `json:"timestamp" db:"timestamp" beam:"timestamp"`
}

// UserSessionSummary represents an aggregated session window stored in PostgreSQL.
type UserSessionSummary struct {
	UserID          string    `json:"user_id" db:"user_id" beam:"user_id"`
	SessionStart    time.Time `json:"session_start" db:"session_start" beam:"session_start"`
	SessionEnd      time.Time `json:"session_end" db:"session_end" beam:"session_end"`
	DurationSeconds int64     `json:"duration_seconds" db:"duration_seconds" beam:"duration_seconds"`
	EventCount      int64     `json:"event_count" db:"event_count" beam:"event_count"`
	IsBounce        bool      `json:"is_bounce" db:"is_bounce" beam:"is_bounce"`
}

type keyByUserFn struct{}

func (fn *keyByUserFn) ProcessElement(e ClickEvent, emit func(string, ClickEvent)) {
	emit(e.UserID, e)
}

type sessionizeUserClicksFn struct {
	GapMinutes int `json:"gap_minutes"`
}

func (fn *sessionizeUserClicksFn) ProcessElement(userID string, events func(*ClickEvent) bool, emit func(UserSessionSummary)) {
	var list []ClickEvent
	var e ClickEvent
	for events(&e) {
		list = append(list, e)
	}

	// Sort chronologically by event timestamp
	sort.Slice(list, func(i, j int) bool {
		return list[i].Timestamp.Before(list[j].Timestamp)
	})

	gap := time.Duration(fn.GapMinutes) * time.Minute
	if gap <= 0 {
		gap = 30 * time.Minute
	}

	var currentSessionStart time.Time
	var currentSessionEnd time.Time
	var sessionCount int64

	for i, evt := range list {
		if i == 0 {
			currentSessionStart = evt.Timestamp.UTC()
			currentSessionEnd = evt.Timestamp.UTC()
			sessionCount = 1
			continue
		}

		timeSinceLast := evt.Timestamp.UTC().Sub(currentSessionEnd)
		if timeSinceLast > gap {
			// Inactivity gap exceeded: emit prior session
			duration := int64(currentSessionEnd.Sub(currentSessionStart).Seconds())
			emit(UserSessionSummary{
				UserID:          userID,
				SessionStart:    currentSessionStart,
				SessionEnd:      currentSessionEnd,
				DurationSeconds: duration,
				EventCount:      sessionCount,
				IsBounce:        sessionCount == 1,
			})

			// Start new session
			currentSessionStart = evt.Timestamp.UTC()
			currentSessionEnd = evt.Timestamp.UTC()
			sessionCount = 1
		} else {
			// Extend active session
			currentSessionEnd = evt.Timestamp.UTC()
			sessionCount++
		}
	}

	// Emit trailing session
	if sessionCount > 0 {
		duration := int64(currentSessionEnd.Sub(currentSessionStart).Seconds())
		emit(UserSessionSummary{
			UserID:          userID,
			SessionStart:    currentSessionStart,
			SessionEnd:      currentSessionEnd,
			DurationSeconds: duration,
			EventCount:      sessionCount,
			IsBounce:        sessionCount == 1,
		})
	}
}

func main() {
	flag.Parse()
	beam.Init()

	ctx := context.Background()
	p := beam.NewPipeline()
	s := p.Root()

	base := time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)

	// Sample clicks containing single-click bounces and multi-session interactions
	sampleClicks := []ClickEvent{
		// User 1: Session A (2 events), then 45 min gap, then Session B (1 bounce event)
		{UserID: "USR-01", EventType: "PAGE_VIEW", URL: "/home", Timestamp: base},
		{UserID: "USR-01", EventType: "CLICK", URL: "/pricing", Timestamp: base.Add(5 * time.Minute)},
		{UserID: "USR-01", EventType: "PAGE_VIEW", URL: "/checkout", Timestamp: base.Add(50 * time.Minute)}, // Gap = 45m > 30m
		// User 2: Single bounce visit
		{UserID: "USR-02", EventType: "PAGE_VIEW", URL: "/blog/article-1", Timestamp: base.Add(10 * time.Minute)},
		// User 3: Long session (3 events within 15 min)
		{UserID: "USR-03", EventType: "PAGE_VIEW", URL: "/catalog", Timestamp: base.Add(15 * time.Minute)},
		{UserID: "USR-03", EventType: "PAGE_VIEW", URL: "/catalog/item-42", Timestamp: base.Add(20 * time.Minute)},
		{UserID: "USR-03", EventType: "CLICK", URL: "/cart/add", Timestamp: base.Add(28 * time.Minute)},
	}

	clicksCol := beam.CreateList(s, sampleClicks)

	// 1. Key by UserID
	keyedCol := beam.ParDo(s, &keyByUserFn{}, clicksCol)

	// 2. Group all click events for each user
	groupedCol := beam.GroupByKey(s, keyedCol)

	// 3. Delimit sessions by inactivity threshold
	sessionsCol := beam.ParDo(s, &sessionizeUserClicksFn{GapMinutes: *gapMins}, groupedCol)

	// 4. Sink to PostgreSQL with upsert on (user_id, session_start)
	writeOpts := postgresio.WriteOptions{
		Host:           *host,
		Port:           *port,
		Database:       *database,
		Username:       *username,
		Password:       *password,
		WriteMode:      postgresio.WriteModeUpsert,
		PrimaryKeyCols: []string{"user_id", "session_start"},
		BatchSize:      2000,
	}

	result := postgresio.Write(s, "public.user_session_summaries", writeOpts, sessionsCol)
	_ = result.FailedRows

	if err := beamx.Run(ctx, p); err != nil {
		log.Fatalf("Failed to execute pipeline: %v", err)
	}
	fmt.Println("User sessionization pipeline completed successfully.")
}
