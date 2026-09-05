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

// Package main demonstrates an advanced machine learning feature engineering and normalization
// pipeline using Apache Beam with PostgreSQL.
//
// Pattern:
// Compute global population statistics (min, max, mean, standard deviation) across
// input entities, pass statistics as a side input to a feature transformer DoFn,
// and apply MinMax scaling [0, 1] and Z-score standardization to generate normalized features.
// ML training and inference pipelines require numeric features scaled into bounded
// distributions (Min-Max normalization [0, 1] and Z-Score standardization) to prevent
// gradient divergence and weight dominance.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math"
	"reflect"
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
)

func init() {
	beam.RegisterType(reflect.TypeOf((*RawEntity)(nil)).Elem())
	beam.RegisterType(reflect.TypeOf((*PopulationStats)(nil)).Elem())
	beam.RegisterType(reflect.TypeOf((*EngineeredFeatures)(nil)).Elem())
	beam.RegisterDoFn(&computeStatsFn{})
	beam.RegisterDoFn(&normalizeFeaturesFn{})
}

// RawEntity represents unnormalized raw features extracted from PostgreSQL.
type RawEntity struct {
	EntityID  string  `json:"entity_id" db:"entity_id" beam:"entity_id"`
	RawIncome float64 `json:"raw_income" db:"raw_income" beam:"raw_income"`
	RawScore  float64 `json:"raw_score" db:"raw_score" beam:"raw_score"`
}

// PopulationStats holds population summary metrics used for feature scaling.
type PopulationStats struct {
	MinIncome    float64 `json:"min_income" beam:"min_income"`
	MaxIncome    float64 `json:"max_income" beam:"max_income"`
	MeanIncome   float64 `json:"mean_income" beam:"mean_income"`
	StdDevIncome float64 `json:"std_dev_income" beam:"std_dev_income"`
	MinScore     float64 `json:"min_score" beam:"min_score"`
	MaxScore     float64 `json:"max_score" beam:"max_score"`
}

// EngineeredFeatures represents normalized and standardized features written to the feature store.
type EngineeredFeatures struct {
	EntityID     string    `json:"entity_id" db:"entity_id" beam:"entity_id"`
	RawIncome    float64   `json:"raw_income" db:"raw_income" beam:"raw_income"`
	NormIncome   float64   `json:"norm_income" db:"norm_income" beam:"norm_income"`
	ZIncome      float64   `json:"z_income" db:"z_income" beam:"z_income"`
	RawScore     float64   `json:"raw_score" db:"raw_score" beam:"raw_score"`
	NormScore    float64   `json:"norm_score" db:"norm_score" beam:"norm_score"`
	EngineeredAt time.Time `json:"engineered_at" db:"engineered_at" beam:"engineered_at"`
}

type computeStatsFn struct{}

func (fn *computeStatsFn) ProcessElement(_ int, entities func(*RawEntity) bool, emit func(PopulationStats)) {
	var count int
	var minInc = math.MaxFloat64
	var maxInc = -math.MaxFloat64
	var sumInc float64

	var minScr = math.MaxFloat64
	var maxScr = -math.MaxFloat64

	var incomes []float64

	var e RawEntity
	for entities(&e) {
		count++
		incomes = append(incomes, e.RawIncome)
		sumInc += e.RawIncome
		if e.RawIncome < minInc {
			minInc = e.RawIncome
		}
		if e.RawIncome > maxInc {
			maxInc = e.RawIncome
		}
		if e.RawScore < minScr {
			minScr = e.RawScore
		}
		if e.RawScore > maxScr {
			maxScr = e.RawScore
		}
	}

	if count == 0 {
		return
	}

	meanInc := sumInc / float64(count)

	var varianceSum float64
	for _, inc := range incomes {
		diff := inc - meanInc
		varianceSum += diff * diff
	}
	stdDev := math.Sqrt(varianceSum / float64(count))
	if stdDev == 0 {
		stdDev = 1.0
	}

	emit(PopulationStats{
		MinIncome:    minInc,
		MaxIncome:    maxInc,
		MeanIncome:   meanInc,
		StdDevIncome: stdDev,
		MinScore:     minScr,
		MaxScore:     maxScr,
	})
}

type normalizeFeaturesFn struct{}

func (fn *normalizeFeaturesFn) ProcessElement(e RawEntity, stats PopulationStats, emit func(EngineeredFeatures)) {
	// MinMax Normalization: (x - min) / (max - min)
	rangeInc := stats.MaxIncome - stats.MinIncome
	if rangeInc == 0 {
		rangeInc = 1.0
	}
	normIncome := (e.RawIncome - stats.MinIncome) / rangeInc

	// Z-Score Standardization: (x - mean) / stddev
	zIncome := (e.RawIncome - stats.MeanIncome) / stats.StdDevIncome

	rangeScr := stats.MaxScore - stats.MinScore
	if rangeScr == 0 {
		rangeScr = 1.0
	}
	normScore := (e.RawScore - stats.MinScore) / rangeScr

	emit(EngineeredFeatures{
		EntityID:     e.EntityID,
		RawIncome:    e.RawIncome,
		NormIncome:   math.Round(normIncome*10000) / 10000,
		ZIncome:      math.Round(zIncome*10000) / 10000,
		RawScore:     e.RawScore,
		NormScore:    math.Round(normScore*10000) / 10000,
		EngineeredAt: time.Now().UTC(),
	})
}

func main() {
	flag.Parse()
	beam.Init()

	ctx := context.Background()
	p := beam.NewPipeline()
	s := p.Root()

	sampleEntities := []RawEntity{
		{EntityID: "ENT-001", RawIncome: 45000.00, RawScore: 620.00},
		{EntityID: "ENT-002", RawIncome: 85000.00, RawScore: 710.00},
		{EntityID: "ENT-003", RawIncome: 125000.00, RawScore: 790.00},
		{EntityID: "ENT-004", RawIncome: 30000.00, RawScore: 580.00},
		{EntityID: "ENT-005", RawIncome: 210000.00, RawScore: 840.00},
	}

	entitiesCol := beam.CreateList(s, sampleEntities)

	// 1. Compute global population stats across the batch
	singleton := beam.Create(s, 1)
	statsCol := beam.ParDo(s, &computeStatsFn{}, singleton, beam.SideInput{Input: entitiesCol})

	// 2. Transform raw features using population statistics side input
	engineeredCol := beam.ParDo(s, &normalizeFeaturesFn{}, entitiesCol, beam.SideInput{Input: statsCol})

	// 3. Sink engineered feature records into PostgreSQL ML feature store
	writeOpts := postgresio.WriteOptions{
		Host:           *host,
		Port:           *port,
		Database:       *database,
		Username:       *username,
		Password:       *password,
		WriteMode:      postgresio.WriteModeUpsert,
		PrimaryKeyCols: []string{"entity_id"},
		BatchSize:      2000,
	}

	result := postgresio.Write(s, "public.ml_feature_store", writeOpts, engineeredCol)
	_ = result.FailedRows

	if err := beamx.Run(ctx, p); err != nil {
		log.Fatalf("Failed to execute pipeline: %v", err)
	}
	fmt.Println("Machine learning feature engineering pipeline completed successfully.")
}
