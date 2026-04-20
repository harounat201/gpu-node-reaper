package controller

import (
	"context"
	"fmt"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	trainingv1alpha1 "github.com/harounat201/gpu-node-reaper/api/v1alpha1"
)

const (
	StateActive      = "ACTIVE"
	StateCompleting  = "COMPLETING"
	StateReclaimable = "RECLAIMABLE"
	StateReleased    = "RELEASED"

	StateAnnotation         = "reaper.harouna.dev/state"
	StallTimeout            = 10 * time.Minute
	UtilizationThreshold    = 0.3
	ConsolidationAnnotation = "reaper.harouna.dev/consolidation-candidate"
)

type TrainingJobReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=training.harouna.dev,resources=trainingjobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=training.harouna.dev,resources=trainingjobs/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=training.harouna.dev,resources=trainingjobs/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=nodes,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch

func (r *TrainingJobReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	log.Info("Reconciling", "job", req.Name)

	// 1. Fetch the TrainingJob
	var job trainingv1alpha1.TrainingJob
	if err := r.Get(ctx, req.NamespacedName, &job); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// 2. List all pods belonging to this job
	var podList corev1.PodList
	if err := r.List(ctx, &podList, client.MatchingLabels{
		"training-job-id": job.Name,
	}); err != nil {
		return ctrl.Result{}, fmt.Errorf("listing pods: %w", err)
	}

	// 3. Classify pod state
	var runningPods, succeededPods, failedPods int
	nodeNames := map[string]bool{}
	for _, pod := range podList.Items {
		switch pod.Status.Phase {
		case corev1.PodRunning:
			runningPods++
			if pod.Spec.NodeName != "" {
				nodeNames[pod.Spec.NodeName] = true
			}
		case corev1.PodSucceeded:
			succeededPods++
		case corev1.PodFailed:
			failedPods++
		}
	}

	log.Info("Pod summary",
		"running", runningPods,
		"succeeded", succeededPods,
		"failed", failedPods,
		"nodes", len(nodeNames),
	)

	// 4. List GPU nodes
	var nodeList corev1.NodeList
	if err := r.List(ctx, &nodeList, client.MatchingLabels{
		"gpu-node": "true",
	}); err != nil {
		return ctrl.Result{}, fmt.Errorf("listing gpu nodes: %w", err)
	}

	// 5. Drive state machine for each GPU node
	for _, node := range nodeList.Items {
		node := node
		currentState := node.Annotations[StateAnnotation]

		if nodeNames[node.Name] {
			// Node has active training pods — ensure ACTIVE
			if err := r.transitionTo(ctx, &node, StateActive, log); err != nil {
				return ctrl.Result{}, err
			}
			continue
		}

		// No active pods on this node
		switch currentState {
		case StateActive:
			// Job just completed or stalled — move to COMPLETING
			log.Info("Job completed or stalled, beginning reclamation", "node", node.Name)
			if err := r.transitionTo(ctx, &node, StateCompleting, log); err != nil {
				return ctrl.Result{}, err
			}

		case StateCompleting:
			// Cordon first
			if !node.Spec.Unschedulable {
				node.Spec.Unschedulable = true
				if err := r.Update(ctx, &node); err != nil {
					return ctrl.Result{}, fmt.Errorf("cordoning node %s: %w", node.Name, err)
				}
				log.Info("Cordoned node", "node", node.Name)
			}

			// Evict all non-DaemonSet pods
			var podList corev1.PodList
			if err := r.List(ctx, &podList, client.MatchingFields{
				"spec.nodeName": node.Name,
			}); err != nil {
				return ctrl.Result{}, fmt.Errorf("listing pods on node %s: %w", node.Name, err)
			}

			allEvicted := true
			for _, pod := range podList.Items {
				pod := pod

				// Skip DaemonSet pods
				if isDaemonSetPod(&pod) {
					continue
				}

				// Skip already terminating pods
				if pod.DeletionTimestamp != nil {
					allEvicted = false
					continue
				}

				eviction := &policyv1.Eviction{
					ObjectMeta: metav1.ObjectMeta{
						Name:      pod.Name,
						Namespace: pod.Namespace,
					},
				}
				if err := r.Client.SubResource("eviction").Create(ctx, &pod, eviction); err != nil {
					if !apierrors.IsNotFound(err) && !apierrors.IsTooManyRequests(err) {
						return ctrl.Result{}, fmt.Errorf("evicting pod %s: %w", pod.Name, err)
					}
				}
				log.Info("Evicted pod", "pod", pod.Name, "node", node.Name)
				allEvicted = false
			}

			// Only advance to RECLAIMABLE when all pods are gone
			if !allEvicted {
				log.Info("Waiting for pods to finish evicting", "node", node.Name)
				return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
			}

			if err := r.transitionTo(ctx, &node, StateReclaimable, log); err != nil {
				return ctrl.Result{}, err
			}

		case StateReclaimable:
			// Remove do-not-disrupt, release to Karpenter
			if err := r.transitionTo(ctx, &node, StateReleased, log); err != nil {
				return ctrl.Result{}, err
			}

		case StateReleased:
			log.Info("Node released to Karpenter", "node", node.Name)
			if err := r.checkUtilization(ctx, &node, log); err != nil {
				return ctrl.Result{}, err
			}
		}
	}

	// 6. Requeue to check for stalls
	return ctrl.Result{RequeueAfter: StallTimeout}, nil
}

func (r *TrainingJobReconciler) transitionTo(ctx context.Context, node *corev1.Node, state string, log logr.Logger) error {
	if node.Annotations == nil {
		node.Annotations = map[string]string{}
	}

	if node.Annotations[StateAnnotation] == state {
		return nil // already in this state, no-op
	}

	previous := node.Annotations[StateAnnotation]
	node.Annotations[StateAnnotation] = state

	switch state {
	case StateActive:
		node.Annotations["karpenter.sh/do-not-disrupt"] = "true"
	case StateReleased:
		delete(node.Annotations, "karpenter.sh/do-not-disrupt")
	}

	if err := r.Update(ctx, node); err != nil {
		return fmt.Errorf("transitioning node %s to %s: %w", node.Name, state, err)
	}

	log.Info("Node state transition", "node", node.Name, "from", previous, "to", state)
	return nil
}

func isDaemonSetPod(pod *corev1.Pod) bool {
	for _, ref := range pod.OwnerReferences {
		if ref.Kind == "DaemonSet" {
			return true
		}
	}
	return false
}

func (r *TrainingJobReconciler) checkUtilization(ctx context.Context, node *corev1.Node, log logr.Logger) error {
	var podList corev1.PodList
	if err := r.List(ctx, &podList, client.MatchingFields{
		"spec.nodeName": node.Name,
	}); err != nil {
		return fmt.Errorf("listing pods for utilization: %w", err)
	}

	// Sum requested GPU across all non-DaemonSet pods
	totalRequestedGPU := int64(0)
	for _, pod := range podList.Items {
		if isDaemonSetPod(&pod) {
			continue
		}
		for _, container := range pod.Spec.Containers {
			if gpu, ok := container.Resources.Requests["nvidia.com/gpu"]; ok {
				totalRequestedGPU += gpu.Value()
			}
		}
	}

	// Get allocatable GPUs on this node
	allocatableGPU := int64(0)
	if gpu, ok := node.Status.Allocatable["nvidia.com/gpu"]; ok {
		allocatableGPU = gpu.Value()
	}

	// In kind, no real GPUs exist — use CPU as proxy
	if allocatableGPU == 0 {
		allocatableGPU = node.Status.Allocatable.Cpu().MilliValue()
		totalRequestedGPU = 0
		for _, pod := range podList.Items {
			if isDaemonSetPod(&pod) {
				continue
			}
			for _, container := range pod.Spec.Containers {
				totalRequestedGPU += container.Resources.Requests.Cpu().MilliValue()
			}
		}
	}

	if allocatableGPU == 0 {
		return nil
	}

	utilization := float64(totalRequestedGPU) / float64(allocatableGPU)
	log.Info("Node utilization", "node", node.Name,
		"requested", totalRequestedGPU,
		"allocatable", allocatableGPU,
		"utilization", fmt.Sprintf("%.1f%%", utilization*100),
	)

	if node.Annotations == nil {
		node.Annotations = map[string]string{}
	}

	if utilization < UtilizationThreshold {
		if node.Annotations[ConsolidationAnnotation] != "true" {
			node.Annotations[ConsolidationAnnotation] = "true"
			if err := r.Update(ctx, node); err != nil {
				return fmt.Errorf("marking consolidation candidate: %w", err)
			}
			log.Info("Marked node as consolidation candidate", "node", node.Name,
				"utilization", fmt.Sprintf("%.1f%%", utilization*100))
		}
	} else {
		if node.Annotations[ConsolidationAnnotation] == "true" {
			delete(node.Annotations, ConsolidationAnnotation)
			if err := r.Update(ctx, node); err != nil {
				return fmt.Errorf("clearing consolidation candidate: %w", err)
			}
			log.Info("Cleared consolidation candidate", "node", node.Name)
		}
	}

	return nil
}

func (r *TrainingJobReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if err := mgr.GetFieldIndexer().IndexField(
		context.Background(),
		&corev1.Pod{},
		"spec.nodeName",
		func(obj client.Object) []string {
			pod := obj.(*corev1.Pod)
			return []string{pod.Spec.NodeName}
		},
	); err != nil {
		return err
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&trainingv1alpha1.TrainingJob{}).
		Watches(
			&corev1.Pod{},
			handler.EnqueueRequestsFromMapFunc(r.podToTrainingJob),
		).
		Named("trainingjob").
		Complete(r)
}

func (r *TrainingJobReconciler) podToTrainingJob(ctx context.Context, obj client.Object) []reconcile.Request {
	jobID, ok := obj.GetLabels()["training-job-id"]
	if !ok {
		return nil
	}
	return []reconcile.Request{
		{NamespacedName: client.ObjectKey{Name: jobID, Namespace: obj.GetNamespace()}},
	}
}
