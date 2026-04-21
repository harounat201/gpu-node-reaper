package controller

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	nodesProtectedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "reaper_nodes_protected_total",
		Help: "Total number of times a node was annotated with do-not-disrupt",
	})

	nodesReleasedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "reaper_nodes_released_total",
		Help: "Total number of times a node was released to Karpenter",
	})

	reclamationDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "reaper_reclamation_duration_seconds",
		Help:    "Time in seconds from ACTIVE to RELEASED",
		Buckets: prometheus.DefBuckets,
	})

	consolidationCandidates = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "reaper_consolidation_candidates",
		Help: "Current number of nodes flagged as consolidation candidates",
	})
)
