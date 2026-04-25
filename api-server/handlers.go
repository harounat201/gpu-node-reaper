package apiserver

import (
	"context"
	"net/http"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	stateAnnotation         = "reaper.harouna.dev/state"
	activeSinceAnnotation   = "reaper.harouna.dev/active-since"
	consolidationAnnotation = "reaper.harouna.dev/consolidation-candidate"
	doNotDisruptAnnotation  = "karpenter.sh/do-not-disrupt"
)

// NodeResponse is the JSON representation of a GPU node's reaper state.
type NodeResponse struct {
	Name                   string `json:"name"`
	State                  string `json:"state"`
	Cordoned               bool   `json:"cordoned"`
	ActiveSince            string `json:"activeSince,omitempty"`
	ConsolidationCandidate bool   `json:"consolidationCandidate"`
	DoNotDisrupt           bool   `json:"doNotDisrupt"`
	PodCount               int    `json:"podCount"`
}

// StatsResponse aggregates cluster-wide GPU node counts.
type StatsResponse struct {
	Total                   int            `json:"total"`
	ByState                 map[string]int `json:"byState"`
	Cordoned                int            `json:"cordoned"`
	ConsolidationCandidates int            `json:"consolidationCandidates"`
}

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleNodes(w http.ResponseWriter, r *http.Request) {
	nodes, err := s.listGPUNodes(r.Context())
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, nodes)
}

func (s *Server) handleNode(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	var node corev1.Node
	if err := s.client.Get(r.Context(), client.ObjectKey{Name: name}, &node); err != nil {
		if apierrors.IsNotFound(err) {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "node not found"})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if node.Labels["gpu-node"] != "true" {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "node not found"})
		return
	}
	writeJSON(w, http.StatusOK, s.nodeToResponse(r.Context(), &node))
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	nodes, err := s.listGPUNodes(r.Context())
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	stats := StatsResponse{
		Total:   len(nodes),
		ByState: map[string]int{"ACTIVE": 0, "COMPLETING": 0, "RECLAIMABLE": 0, "RELEASED": 0},
	}
	for _, n := range nodes {
		if _, ok := stats.ByState[n.State]; ok {
			stats.ByState[n.State]++
		}
		if n.Cordoned {
			stats.Cordoned++
		}
		if n.ConsolidationCandidate {
			stats.ConsolidationCandidates++
		}
	}
	writeJSON(w, http.StatusOK, stats)
}

func (s *Server) listGPUNodes(ctx context.Context) ([]NodeResponse, error) {
	var nodeList corev1.NodeList
	if err := s.client.List(ctx, &nodeList, client.MatchingLabels{"gpu-node": "true"}); err != nil {
		return nil, err
	}
	result := make([]NodeResponse, 0, len(nodeList.Items))
	for i := range nodeList.Items {
		result = append(result, s.nodeToResponse(ctx, &nodeList.Items[i]))
	}
	return result, nil
}

func (s *Server) nodeToResponse(ctx context.Context, node *corev1.Node) NodeResponse {
	ann := node.Annotations
	if ann == nil {
		ann = map[string]string{}
	}
	var podList corev1.PodList
	_ = s.client.List(ctx, &podList, client.MatchingFields{"spec.nodeName": node.Name})
	podCount := 0
	for _, pod := range podList.Items {
		if !isDaemonSetPod(&pod) && pod.DeletionTimestamp == nil {
			podCount++
		}
	}
	return NodeResponse{
		Name:                   node.Name,
		State:                  ann[stateAnnotation],
		Cordoned:               node.Spec.Unschedulable,
		ActiveSince:            ann[activeSinceAnnotation],
		ConsolidationCandidate: ann[consolidationAnnotation] == "true",
		DoNotDisrupt:           ann[doNotDisruptAnnotation] == "true",
		PodCount:               podCount,
	}
}

func isDaemonSetPod(pod *corev1.Pod) bool {
	for _, ref := range pod.OwnerReferences {
		if ref.Kind == "DaemonSet" {
			return true
		}
	}
	return false
}
