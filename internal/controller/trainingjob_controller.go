package controller

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
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
	ConsolidationAnnotation = "reaper.harouna.dev/consolidation-candidate"

	defaultStallTimeout          = 10 * time.Minute
	defaultUtilizationThreshold  = 0.30
)

type TrainingJobReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
}

// +kubebuilder:rbac:groups=training.harouna.dev,resources=trainingjobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=training.harouna.dev,resources=trainingjobs/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=training.harouna.dev,resources=trainingjobs/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=nodes,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

func (r *TrainingJobReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	log.Info("Reconciling", "job", req.Name)

	// 1. Fetch the TrainingJob
	var job trainingv1alpha1.TrainingJob
	if err := r.Get(ctx, req.NamespacedName, &job); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// 2. Resolve per-job timeouts and thresholds
	stallTimeout := defaultStallTimeout
	if job.Spec.StallTimeout != nil && job.Spec.StallTimeout.Duration > 0 {
		stallTimeout = job.Spec.StallTimeout.Duration
	}
	utilizationThreshold := defaultUtilizationThreshold
	if job.Spec.UtilizationThreshold != nil {
		utilizationThreshold = float64(*job.Spec.UtilizationThreshold) / 100.0
	}

	// 3. List pods matching the job's PodSelector
	sel, err := metav1.LabelSelectorAsSelector(&job.Spec.PodSelector)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("invalid pod selector: %w", err)
	}
	var podList corev1.PodList
	if err := r.List(ctx, &podList,
		client.InNamespace(req.Namespace),
		client.MatchingLabelsSelector{Selector: sel},
	); err != nil {
		return ctrl.Result{}, fmt.Errorf("listing pods: %w", err)
	}

	// 4. Classify pod state
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

	// 5. List GPU nodes
	var nodeList corev1.NodeList
	if err := r.List(ctx, &nodeList, client.MatchingLabels{"gpu-node": "true"}); err != nil {
		return ctrl.Result{}, fmt.Errorf("listing gpu nodes: %w", err)
	}

	// 6. Drive state machine for each GPU node
	for _, node := range nodeList.Items {
		node := node
		currentState := node.Annotations[StateAnnotation]

		if nodeNames[node.Name] {
			if err := r.transitionTo(ctx, &node, StateActive, log); err != nil {
				return ctrl.Result{}, err
			}
			continue
		}

		switch currentState {
		case StateActive:
			log.Info("Job completed or stalled, beginning reclamation", "node", node.Name)
			if err := r.transitionTo(ctx, &node, StateCompleting, log); err != nil {
				return ctrl.Result{}, err
			}

		case StateCompleting:
			if !node.Spec.Unschedulable {
				node.Spec.Unschedulable = true
				if err := r.Update(ctx, &node); err != nil {
					return ctrl.Result{}, fmt.Errorf("cordoning node %s: %w", node.Name, err)
				}
				log.Info("Cordoned node", "node", node.Name)
				if r.Recorder != nil {
					r.Recorder.Eventf(&node, corev1.EventTypeNormal, "NodeCordoned",
						"Cordoned node for reclamation")
				}
			}

			var nodePods corev1.PodList
			if err := r.List(ctx, &nodePods, client.MatchingFields{
				"spec.nodeName": node.Name,
			}); err != nil {
				return ctrl.Result{}, fmt.Errorf("listing pods on node %s: %w", node.Name, err)
			}

			allEvicted := true
			for _, pod := range nodePods.Items {
				pod := pod
				if isDaemonSetPod(&pod) {
					continue
				}
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
				if r.Recorder != nil {
					r.Recorder.Eventf(&pod, corev1.EventTypeNormal, "PodEvicted",
						"Pod evicted by gpu-node-reaper for node reclamation")
				}
				allEvicted = false
			}

			if !allEvicted {
				log.Info("Waiting for pods to finish evicting", "node", node.Name)
				return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
			}

			if err := r.transitionTo(ctx, &node, StateReclaimable, log); err != nil {
				return ctrl.Result{}, err
			}

		case StateReclaimable:
			if err := r.transitionTo(ctx, &node, StateReleased, log); err != nil {
				return ctrl.Result{}, err
			}

		case StateReleased:
			log.Info("Node released to Karpenter", "node", node.Name)
			if err := r.checkUtilization(ctx, &node, utilizationThreshold, log); err != nil {
				return ctrl.Result{}, err
			}
		}
	}

	// 7. Update job status
	if err := r.updateStatus(ctx, &job, runningPods, nodeNames); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{RequeueAfter: stallTimeout}, nil
}

func (r *TrainingJobReconciler) updateStatus(
	ctx context.Context,
	job *trainingv1alpha1.TrainingJob,
	runningPods int,
	nodeNames map[string]bool,
) error {
	nodeSlice := make([]string, 0, len(nodeNames))
	for n := range nodeNames {
		nodeSlice = append(nodeSlice, n)
	}
	sort.Strings(nodeSlice)

	var phase trainingv1alpha1.TrainingJobPhase
	switch {
	case runningPods > 0:
		phase = trainingv1alpha1.PhaseRunning
	case job.Status.Phase == trainingv1alpha1.PhaseRunning ||
		job.Status.Phase == trainingv1alpha1.PhaseCompleting:
		phase = trainingv1alpha1.PhaseCompleting
	default:
		phase = trainingv1alpha1.PhasePending
	}

	if job.Status.Phase == phase && stringSlicesEqual(job.Status.Nodes, nodeSlice) {
		return nil
	}

	job.Status.Phase = phase
	job.Status.Nodes = nodeSlice
	if phase == trainingv1alpha1.PhaseRunning && job.Status.StartTime == nil {
		now := metav1.Now()
		job.Status.StartTime = &now
	}
	if err := r.Status().Update(ctx, job); err != nil {
		return fmt.Errorf("updating job status: %w", err)
	}
	return nil
}

func (r *TrainingJobReconciler) transitionTo(ctx context.Context, node *corev1.Node, state string, log logr.Logger) error {
	if node.Annotations == nil {
		node.Annotations = map[string]string{}
	}
	if node.Annotations[StateAnnotation] == state {
		return nil
	}

	previous := node.Annotations[StateAnnotation]
	node.Annotations[StateAnnotation] = state

	switch state {
	case StateActive:
		node.Annotations["karpenter.sh/do-not-disrupt"] = "true"
		node.Annotations["reaper.harouna.dev/active-since"] = time.Now().UTC().Format(time.RFC3339)
		nodesProtectedTotal.Inc()
	case StateReleased:
		delete(node.Annotations, "karpenter.sh/do-not-disrupt")
		nodesReleasedTotal.Inc()
		if activeSince, ok := node.Annotations["reaper.harouna.dev/active-since"]; ok {
			if t, err := time.Parse(time.RFC3339, activeSince); err == nil {
				reclamationDuration.Observe(time.Since(t).Seconds())
			}
			delete(node.Annotations, "reaper.harouna.dev/active-since")
		}
	}

	if err := r.Update(ctx, node); err != nil {
		return fmt.Errorf("transitioning node %s to %s: %w", node.Name, state, err)
	}

	log.Info("Node state transition", "node", node.Name, "from", previous, "to", state)
	if r.Recorder != nil {
		r.Recorder.Eventf(node, corev1.EventTypeNormal, "StateTransition",
			"Transitioned from %s to %s", previous, state)
	}
	return nil
}

func (r *TrainingJobReconciler) checkUtilization(
	ctx context.Context,
	node *corev1.Node,
	threshold float64,
	log logr.Logger,
) error {
	var podList corev1.PodList
	if err := r.List(ctx, &podList, client.MatchingFields{
		"spec.nodeName": node.Name,
	}); err != nil {
		return fmt.Errorf("listing pods for utilization: %w", err)
	}

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

	allocatableGPU := int64(0)
	if gpu, ok := node.Status.Allocatable["nvidia.com/gpu"]; ok {
		allocatableGPU = gpu.Value()
	}

	// Kind dev clusters have no real GPUs — fall back to CPU as a proxy
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

	if utilization < threshold {
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

	if node.Annotations[ConsolidationAnnotation] == "true" {
		consolidationCandidates.Inc()
	} else {
		consolidationCandidates.Dec()
	}

	return nil
}

func (r *TrainingJobReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if err := mgr.GetFieldIndexer().IndexField(
		context.Background(),
		&corev1.Pod{},
		"spec.nodeName",
		func(obj client.Object) []string {
			return []string{obj.(*corev1.Pod).Spec.NodeName}
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
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		return nil
	}
	var jobList trainingv1alpha1.TrainingJobList
	if err := r.List(ctx, &jobList, client.InNamespace(pod.Namespace)); err != nil {
		return nil
	}
	var requests []reconcile.Request
	for _, job := range jobList.Items {
		sel, err := metav1.LabelSelectorAsSelector(&job.Spec.PodSelector)
		if err != nil {
			continue
		}
		if sel.Matches(labels.Set(pod.Labels)) {
			requests = append(requests, reconcile.Request{
				NamespacedName: client.ObjectKey{Name: job.Name, Namespace: job.Namespace},
			})
		}
	}
	return requests
}

func isDaemonSetPod(pod *corev1.Pod) bool {
	for _, ref := range pod.OwnerReferences {
		if ref.Kind == "DaemonSet" {
			return true
		}
	}
	return false
}

func stringSlicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
