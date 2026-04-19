package controller

import (
	"context"
	"fmt"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
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

	StateAnnotation = "reaper.harouna.dev/state"
	StallTimeout    = 10 * time.Minute
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
			// Cordon the node
			if !node.Spec.Unschedulable {
				node.Spec.Unschedulable = true
				if err := r.Update(ctx, &node); err != nil {
					return ctrl.Result{}, fmt.Errorf("cordoning node %s: %w", node.Name, err)
				}
				log.Info("Cordoned node", "node", node.Name)
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
			// Nothing to do — Karpenter takes it from here
			log.Info("Node released to Karpenter", "node", node.Name)
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

func (r *TrainingJobReconciler) SetupWithManager(mgr ctrl.Manager) error {
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
