/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TrainingJobPhase represents the lifecycle phase of a TrainingJob.
type TrainingJobPhase string

const (
	PhasePending    TrainingJobPhase = "Pending"
	PhaseRunning    TrainingJobPhase = "Running"
	PhaseCompleting TrainingJobPhase = "Completing"
	PhaseCompleted  TrainingJobPhase = "Completed"
)

// TrainingJobSpec defines the desired policy for a distributed training job.
type TrainingJobSpec struct {
	// PodSelector selects the pods that belong to this training job.
	// The controller protects GPU nodes running matching pods from Karpenter disruption
	// and reclaims those nodes once the job completes.
	// +kubebuilder:validation:Required
	PodSelector metav1.LabelSelector `json:"podSelector"`

	// DrainTimeout is the maximum time to wait for pods to terminate before
	// force-evicting during node reclamation. Defaults to 5m.
	// +optional
	DrainTimeout *metav1.Duration `json:"drainTimeout,omitempty"`

	// StallTimeout is how long a node can remain in COMPLETING state with no
	// active pods before being force-reclaimed. Defaults to 10m.
	// +optional
	StallTimeout *metav1.Duration `json:"stallTimeout,omitempty"`

	// UtilizationThreshold is the GPU utilization percentage (0–100) below which
	// a released node is flagged as a Karpenter consolidation candidate.
	// +kubebuilder:default=30
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=100
	// +optional
	UtilizationThreshold *int32 `json:"utilizationThreshold,omitempty"`
}

// TrainingJobStatus defines the observed state of TrainingJob.
type TrainingJobStatus struct {
	// Phase is the current lifecycle phase of the training job.
	// +optional
	Phase TrainingJobPhase `json:"phase,omitempty"`

	// Nodes lists the GPU node names where this job's pods are running.
	// +optional
	Nodes []string `json:"nodes,omitempty"`

	// StartTime is when the job first transitioned to Running.
	// +optional
	StartTime *metav1.Time `json:"startTime,omitempty"`

	// Conditions represent the current state of the TrainingJob resource.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=".status.phase"
// +kubebuilder:printcolumn:name="Nodes",type=string,JSONPath=".status.nodes"
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=".metadata.creationTimestamp"

// TrainingJob declares a distributed training workload whose GPU nodes should be
// protected from Karpenter disruption during training and reclaimed promptly after.
type TrainingJob struct {
	metav1.TypeMeta `json:",inline"`

	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// +optional
	Spec TrainingJobSpec `json:"spec,omitempty"`

	// +optional
	Status TrainingJobStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// TrainingJobList contains a list of TrainingJob.
type TrainingJobList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []TrainingJob `json:"items"`
}

func init() {
	SchemeBuilder.Register(&TrainingJob{}, &TrainingJobList{})
}
