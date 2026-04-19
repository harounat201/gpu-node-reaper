package controller

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	trainingv1alpha1 "github.com/harounat201/gpu-node-reaper/api/v1alpha1"
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

	log.Info("Found pods", "count", len(podList.Items))

	// 3. Collect nodes running active pods
	nodeNames := map[string]bool{}
	for _, pod := range podList.Items {
		if pod.Status.Phase == corev1.PodRunning && pod.Spec.NodeName != "" {
			nodeNames[pod.Spec.NodeName] = true
		}
	}

	// 4. Annotate active nodes with do-not-disrupt
	for nodeName := range nodeNames {
		var node corev1.Node
		if err := r.Get(ctx, client.ObjectKey{Name: nodeName}, &node); err != nil {
			return ctrl.Result{}, fmt.Errorf("getting node %s: %w", nodeName, err)
		}

		if node.Annotations == nil {
			node.Annotations = map[string]string{}
		}

		if node.Annotations["karpenter.sh/do-not-disrupt"] != "true" {
			node.Annotations["karpenter.sh/do-not-disrupt"] = "true"
			if err := r.Update(ctx, &node); err != nil {
				return ctrl.Result{}, fmt.Errorf("annotating node %s: %w", nodeName, err)
			}
			log.Info("Protected node", "node", nodeName)
		}
	}

	// 5. If no active pods, remove do-not-disrupt from all gpu nodes
	if len(nodeNames) == 0 {
		var nodeList corev1.NodeList
		if err := r.List(ctx, &nodeList, client.MatchingLabels{
			"gpu-node": "true",
		}); err != nil {
			return ctrl.Result{}, fmt.Errorf("listing nodes: %w", err)
		}

		for _, node := range nodeList.Items {
			if node.Annotations["karpenter.sh/do-not-disrupt"] == "true" {
				delete(node.Annotations, "karpenter.sh/do-not-disrupt")
				if err := r.Update(ctx, &node); err != nil {
					return ctrl.Result{}, fmt.Errorf("removing annotation from node %s: %w", node.Name, err)
				}
				log.Info("Released node", "node", node.Name)
			}
		}
	}

	return ctrl.Result{}, nil
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
