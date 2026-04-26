# gpu-node-reaper: Reaper

Harness the power of Kind (Kubernetes in Docker) to support distributed ML training workflows on AWS EKS. Reaper is a custom policy layer demo designed to 1) protect provisioned gang training nodes from disruption by Karpenter on EKS and 2) support horizontal autoscaling in these workflows by automatically facilitating first-fit pod bin-packing and node consolidation.

In short: A Kubernetes controller that manages GPU node lifecycle on Karpenter-managed EKS clusters.

**The problem:** Karpenter is great at provisioning and terminating nodes, but it has no awareness of distributed training jobs. It can consolidate a node mid-training and kill your 8-hour PyTorch run. And after a job finishes, idle GPU nodes can sit unclaimed for minutes while Karpenter waits for its consolidation window.

**What this does:** You declare a `TrainingJob` resource pointing at your training pods. The controller:
1. Applies `karpenter.sh/do-not-disrupt` to GPU nodes running your pods — Karpenter will not touch them
2. Detects when the job completes (or stalls), cordons the nodes, and drains remaining pods
3. Removes the annotation so Karpenter can reclaim the nodes immediately
4. Flags underutilized nodes as consolidation candidates

## Install

```bash
kubectl apply -f https://github.com/harounat201/gpu-node-reaper/releases/latest/download/install.yaml
```

That's it. No Helm required. The CRD, RBAC, and controller Deployment are all in that file.

## Quick start

Label your GPU nodes so the controller can find them:

```bash
kubectl label node <your-gpu-node> gpu-node=true
```

Declare a TrainingJob that points at your training pods:

```yaml
apiVersion: training.harouna.dev/v1alpha1
kind: TrainingJob
metadata:
  name: my-training-run
  namespace: default
spec:
  podSelector:
    matchLabels:
      app: my-trainer
      role: worker
```

Apply it before (or when) you launch your training pods:

```bash
kubectl apply -f trainingjob.yaml
kubectl apply -f training-pods.yaml
```

Watch the lifecycle:

```bash
kubectl get trainingjob my-training-run -w
# NAME               PHASE       NODES                    AGE
# my-training-run    Running     ["gpu-node-1","gpu-node-2"]   2m

kubectl get events --field-selector involvedObject.name=gpu-node-1
# StateTransition: Transitioned from  to ACTIVE
# StateTransition: Transitioned from ACTIVE to COMPLETING
# NodeCordoned: Cordoned node for reclamation
# StateTransition: Transitioned from COMPLETING to RECLAIMABLE
# StateTransition: Transitioned from RECLAIMABLE to RELEASED
```

## How it works

The controller watches for pods matching your `podSelector` on nodes labeled `gpu-node=true`. It drives each node through a state machine:

```
ACTIVE → COMPLETING → RECLAIMABLE → RELEASED
```

| State | What's happening | Karpenter annotation |
|---|---|---|
| `ACTIVE` | Training pods are running | `do-not-disrupt: true` |
| `COMPLETING` | Pods finished — cordoning and draining | `do-not-disrupt: true` |
| `RECLAIMABLE` | Node is empty and safe to reclaim | removed |
| `RELEASED` | Handed back to Karpenter | removed |

Gang failure detection: if all pods disappear simultaneously (eviction or node failure), the controller detects the stall and drives the node to `RELEASED` after `stallTimeout`.

## TrainingJob spec

```yaml
spec:
  # Required: selects the pods that belong to this job
  podSelector:
    matchLabels:
      app: my-trainer

  # How long to wait for pods to drain before force-evicting (default: 5m)
  drainTimeout: 5m

  # How long a node can sit with no pods before force-reclaim (default: 10m)
  stallTimeout: 10m

  # GPU utilization % below which a released node is flagged for consolidation (default: 30)
  utilizationThreshold: 30
```

## Prerequisites

- Kubernetes 1.28+
- Karpenter installed (controller is a no-op without it, but works fine)
- GPU nodes labeled `gpu-node=true`

## Local development

```bash
# Start the Kind cluster (Docker must be running)
kind create cluster --name reaper-dev
kubectl label node reaper-dev-worker gpu-node=true
kubectl label node reaper-dev-worker2 gpu-node=true

# Install CRDs and run the controller locally
make install
make run

# In another terminal — apply a sample job
kubectl apply -f config/samples/training_v1alpha1_trainingjob.yaml

# Simulate a training pod on a GPU node
kubectl run worker-0 --image=busybox --labels="app=pytorch-resnet,role=worker" \
  --overrides='{"spec":{"nodeName":"reaper-dev-worker"}}' -- sleep 3600

# Watch node state transitions
kubectl get nodes -o custom-columns=NAME:.metadata.name,STATE:.metadata.annotations.reaper\\.harouna\\.dev/state

# Delete the pod to trigger reclamation
kubectl delete pod worker-0
```

## Releasing

Push a tag to trigger the release workflow:

```bash
git tag v0.1.0
git push origin v0.1.0
```

GitHub Actions builds a multi-arch image (`linux/amd64`, `linux/arm64`), pushes it to GHCR, and attaches `dist/install.yaml` to the GitHub release.

## License

Apache 2.0
