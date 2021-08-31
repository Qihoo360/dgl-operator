/*
Copyright 2021.

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

package controllers

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"sort"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	dglv1a1 "github.com/Qihoo360/dgl-operator/api/v1alpha1"
)

const (
	launcherSuffix         = "-launcher"
	workerSuffix           = "-worker"
	partitionerSuffix      = "-partitioner"
	configSuffix           = "-config"
	kubexecScriptName      = "kubexec.sh"
	kubectlName            = "kubectl"
	hostfileName           = "hostfile"
	partfileName           = "partfile"
	leadfileName           = "leadfile"
	partitionerWatcherName = "watcher-loop-partitioner"
	workerWatcherName      = "watcher-loop-worker"
	kubectlDownloadName    = "kubectl-download"
	partitionCollectName   = "partition-collect"

	kubexecPathEnv    = "DGL_OPERATOR_KUBEXEC_PATH"
	hostfilePathEnv   = "DGL_OPERATOR_HOSTFILE_PATH"
	partfilePathEnv   = "DGL_OPERATOR_PARTFILE_PATH"
	kubectlPathEnv    = "DGL_OPERATOR_KUBECTL_PATH"
	kubeEnv           = "DGL_OPERATOR_ENV"
	phaseEnv          = "DGL_OPERATOR_PHASE_ENV"
	containerPortName = "dglserver"

	configVolumeName  = "config-volume"
	configMountPath   = "/etc/dgl"
	kubectlVolumeName = "kube-volume"
	kubectlMountPath  = "/opt/kube"
	memoryVolumeName  = "shm-volume"
	memoryMountPath   = "/dev/shm"
	datasetVolumeName = "dataset-volume"
	datasetMountPath  = "/dgl_workspace/dataset"

	initContainerCpu            = "100m"
	initContainerEphStorage     = "5Gi"
	initContainerMem            = "512Mi"
	defaultLauncherContainerCpu = "1"
	defaultLauncherContainerMem = "2Gi"
)

var (
	jobOwnerKey      = ".metadata.controller"
	apiGVStr         = dglv1a1.GroupVersion.String()
	shmSizeLimitInGB int64
)

// DGLJobReconciler reconciles a DGLJob object
type DGLJobReconciler struct {
	client.Client
	Log                  logr.Logger
	Scheme               *runtime.Scheme
	Recorder             record.EventRecorder
	WatcherLoopImage     string
	KubectlDownloadImage string
}

//+kubebuilder:rbac:groups=qihoo.net,resources=dgljobs,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=qihoo.net,resources=dgljobs/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=qihoo.net,resources=dgljobs/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
func (r *DGLJobReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := r.Log.WithValues("dgljob", req.NamespacedName)

	startTime := time.Now()
	defer func() {
		log.Info("Finished reconciling job", req.NamespacedName.String(), time.Since(startTime))
	}()

	// Load the current DGLJobs
	var dgljob dglv1a1.DGLJob
	if err := r.Get(ctx, req.NamespacedName, &dgljob); err != nil {
		log.Error(err, "unable to fetch DGLJob")
		// we'll ignore not-found errors, since they can't be fixed by an
		// immediate requeue (we'll need to wait for a new notification), and
		// we can get them on deleted requests.
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// TODO(ryantd): specify the reconcile log
	log.Info("Reconcile", "version", dgljob.ResourceVersion)

	// Working on terminating, just return for dgl job that is terminating
	if dgljob.DeletionTimestamp != nil {
		return ctrl.Result{}, nil
	}

	// Working on terminated
	// Whether the job is preempted, and requeue it
	requeue := false
	// If the DGLJob is terminated, delete its pods according to cleanPodPolicy
	if isJobFinished(dgljob.Status) {
		if isJobSucceeded(dgljob.Status) && isCleanUpPods(dgljob.Spec.CleanPodPolicy) {
			// set worker StatefulSet Replicas to 0.
			if err := r.deleteWorkers(&dgljob); err != nil {
				return ctrl.Result{}, err
			}
			initializeDGLJobStatus(&dgljob, dglv1a1.WorkerReplica)
			if isPartitionModeDGLAPI(&dgljob) {
				initializeDGLJobStatus(&dgljob, dglv1a1.PartitionerReplica)
			}
		}
		if isJobFailed(dgljob.Status) && (isJobEvicted(dgljob.Status) || dgljob.Status.CompletionTime == nil) {
			requeue = true
		}
		if !requeue {
			if isJobFailed(dgljob.Status) && isCleanUpPods(dgljob.Spec.CleanPodPolicy) {
				// set worker StatefulSet Replicas to 0.
				if err := r.deleteWorkers(&dgljob); err != nil {
					return ctrl.Result{}, err
				}
			}
			initializeDGLJobStatus(&dgljob, dglv1a1.WorkerReplica)
			initializeDGLJobStatus(&dgljob, dglv1a1.LauncherReplica)
			if isPartitionModeDGLAPI(&dgljob) {
				initializeDGLJobStatus(&dgljob, dglv1a1.PartitionerReplica)
			}
			return ctrl.Result{}, nil
		} else {
			launcher, err := r.getLauncherPod(ctx, &dgljob)
			if err == nil && launcher != nil && isPodFailed(launcher) {
				// In requeue, should delete launcher pod
				err = r.deleteResource(ctx, &dgljob, launcher)
				if err != nil && !errors.IsNotFound(err) {
					r.Log.Error(err, "Failed to delete pod[%s/%s]: %v", dgljob.Namespace, launcher.Name, err)
					return ctrl.Result{}, err
				}
			}
		}
	}

	// Working on taking off, first set StartTime.
	if dgljob.Status.StartTime == nil {
		now := metav1.Now()
		dgljob.Status.StartTime = &now
	}

	if isPartitionModeDGLAPI(&dgljob) {
		partitionerSpec := dgljob.Spec.DGLReplicaSpecs[dglv1a1.PartitionerReplica]
		if partitionerSpec == nil {
			replicas := 1
			replicaSpec := &dglv1a1.ReplicaSpec{}
			replicaSpec.Replicas = &replicas
			dgljob.Spec.DGLReplicaSpecs[dglv1a1.PartitionerReplica] = replicaSpec
		}
	}

	// Get the launcher Pod for this DGLJob
	launcher, err := r.getLauncherPod(ctx, &dgljob)
	if err != nil {
		return ctrl.Result{}, err
	}

	var workers *corev1.PodList
	var partitioners *corev1.PodList
	// We're done if the launcher either succeeded or failed.
	done := launcher != nil && isPodFinished(launcher)
	if !done {
		workerSpec := dgljob.Spec.DGLReplicaSpecs[dglv1a1.WorkerReplica]
		workerReplicas := 0
		if workerSpec != nil && workerSpec.Replicas != nil {
			workerReplicas = *workerSpec.Replicas
		}

		// Get the ConfigMap for this DGLJob.
		config, err := r.getOrCreateConfigMap(&dgljob, workerReplicas)
		if config == nil && err == nil {
			return ctrl.Result{Requeue: true}, nil
		} else if err != nil {
			return ctrl.Result{}, err
		}

		// Get the launcher ServiceAccount for this DGLJob.
		sa, err := r.getOrCreateLauncherServiceAccount(&dgljob)
		if sa == nil && err == nil {
			return ctrl.Result{Requeue: true}, nil
		} else if err != nil {
			return ctrl.Result{}, err
		}

		// Get the launcher Role for this DGLJob.
		role, err := r.getOrCreateLauncherRole(&dgljob, workerReplicas)
		if role == nil && err == nil {
			return ctrl.Result{Requeue: true}, nil
		} else if err != nil {
			return ctrl.Result{}, err
		}

		// Get the launcher RoleBinding for this DGLJob.
		rb, err := r.getOrCreateLauncherRoleBinding(&dgljob)
		if rb == nil && err == nil {
			return ctrl.Result{Requeue: true}, nil
		} else if err != nil {
			return ctrl.Result{}, err
		}

		if isPartitionModeDGLAPI(&dgljob) {
			// Get the partitioner ServiceAccount for this DGLJob.
			sa, err = r.getOrCreatePartitionerServiceAccount(&dgljob)
			if sa == nil && err == nil {
				return ctrl.Result{Requeue: true}, nil
			} else if err != nil {
				return ctrl.Result{}, err
			}

			// Get the partitioner Role for this DGLJob.
			role, err = r.getOrCreatePartitionerRole(&dgljob, workerReplicas)
			if role == nil && err == nil {
				return ctrl.Result{Requeue: true}, nil
			} else if err != nil {
				return ctrl.Result{}, err
			}

			// Get the partitioner RoleBinding for this DGLJob.
			rb, err = r.getOrCreatePartitionerRoleBinding(&dgljob)
			if rb == nil && err == nil {
				return ctrl.Result{Requeue: true}, nil
			} else if err != nil {
				return ctrl.Result{}, err
			}
		}

		// TODO(ryantd): Support Pod Group
		if launcher == nil {
			launcher, err = r.createLauncher(&dgljob)
			if err != nil {
				// r.Recorder.Eventf(dgljob, corev1.EventTypeWarning, string(dglv1a1.Failed), "launcher pod created failed: %v", err)
				return ctrl.Result{}, err
			}
		}
	}
	if isPartitionModeDGLAPI(&dgljob) {
		partitioners, err = r.getOrCreatePartitioners(&dgljob)
		if err != nil {
			return ctrl.Result{}, err
		}
	}

	if dgljob.Status.Phase == dglv1a1.Partitioned ||
		dgljob.Status.Phase == dglv1a1.Training {
		workers, err = r.getOrCreateWorkers(&dgljob)
		if err != nil {
			return ctrl.Result{}, err
		}
	}

	// Finally, we update the status block of the DGLJob resource to reflect the
	// current state of the world.
	latestStatus := r.buildLatestJobStatus(ctx, &dgljob, partitioners, workers, launcher)
	if !reflect.DeepEqual(latestStatus, dgljob.Status) {
		dgljob.Status = latestStatus
		if err := r.Status().Update(ctx, &dgljob); err != nil {
			if errors.IsConflict(err) {
				return ctrl.Result{Requeue: true}, nil
			}
			return ctrl.Result{}, err
		}
	}

	return ctrl.Result{}, nil
}

func (r *DGLJobReconciler) buildLatestJobStatus(
	ctx context.Context,
	dgljob *dglv1a1.DGLJob,
	partitioners *corev1.PodList,
	workers *corev1.PodList,
	launcher *corev1.Pod,
) dglv1a1.DGLJobStatus {
	updateJobStatusOnce := func(rs *dglv1a1.ReplicaStatus, pod *corev1.Pod) {
		if pod.CreationTimestamp.Before(&dgljob.CreationTimestamp) {
			return
		}

		switch pod.Status.Phase {
		case corev1.PodPending:
			rs.Pending++
		case corev1.PodRunning:
			if isPodRealRuning(pod) {
				rs.Running++
			} else {
				rs.Starting++
			}
		case corev1.PodFailed:
			rs.Failed++
		case corev1.PodSucceeded:
			rs.Succeeded++
		}
	}

	childPods := &corev1.PodList{
		Items: []corev1.Pod{},
	}
	if workers != nil {
		childPods.Items = append(childPods.Items, workers.DeepCopy().Items...)
	}
	if partitioners != nil {
		childPods.Items = append(childPods.Items, partitioners.DeepCopy().Items...)
	}
	childPods.Items = append(childPods.Items, *launcher)

	launcherStatus := &dglv1a1.ReplicaStatus{}
	workerStatus := &dglv1a1.ReplicaStatus{}
	partitionerStatus := &dglv1a1.ReplicaStatus{}
	for i, pod := range childPods.Items {
		replicaType := pod.Annotations[dglv1a1.DGLReplicaAnnotation]
		if replicaType == string(dglv1a1.LauncherReplica) {
			updateJobStatusOnce(launcherStatus, &childPods.Items[i])
		} else if replicaType == string(dglv1a1.WorkerReplica) {
			updateJobStatusOnce(workerStatus, &childPods.Items[i])
		} else if replicaType == string(dglv1a1.PartitionerReplica) {
			updateJobStatusOnce(partitionerStatus, &childPods.Items[i])
		}
	}

	currentJobPhase := genJobPhase(dgljob)
	if currentJobPhase != dglv1a1.Pending {
		launcherStatus.Ready = fmt.Sprintf("%d/%d", launcherStatus.Running, *dgljob.Spec.DGLReplicaSpecs[dglv1a1.LauncherReplica].Replicas)
		workerStatus.Ready = fmt.Sprintf("%d/%d", workerStatus.Running, *dgljob.Spec.DGLReplicaSpecs[dglv1a1.WorkerReplica].Replicas)
		partitionerStatus.Ready = fmt.Sprintf("%d/%d", partitionerStatus.Running, *dgljob.Spec.DGLReplicaSpecs[dglv1a1.PartitionerReplica].Replicas)
	}
	completionTime := dgljob.Status.CompletionTime
	if completionTime == nil {
		if currentJobPhase == dglv1a1.Failed || currentJobPhase == dglv1a1.Succeed {
			now := metav1.Now()
			completionTime = &now
		}
	}

	return dglv1a1.DGLJobStatus{
		Phase: currentJobPhase,
		ReplicaStatuses: map[dglv1a1.ReplicaType]*dglv1a1.ReplicaStatus{
			dglv1a1.LauncherReplica:    launcherStatus,
			dglv1a1.WorkerReplica:      workerStatus,
			dglv1a1.PartitionerReplica: partitionerStatus,
		},
		CompletionTime: completionTime,
	}
}

func (r *DGLJobReconciler) deleteResource(ctx context.Context, dgljob *dglv1a1.DGLJob, obj client.Object) error {
	// TODO(ryantd): Recorder.Event will raise an error of nil pointer
	if obj.GetDeletionTimestamp() != nil {
		return nil
	}
	// tp := obj.GetObjectKind().GroupVersionKind().Kind
	if err := r.Delete(ctx, obj, client.PropagationPolicy(metav1.DeletePropagationBackground)); (err) != nil {
		// r.Recorder.Event(dgljob, corev1.EventTypeWarning, "Delete", fmt.Sprintf("delete failed %s %s", tp, obj.GetName()))
		return err
	}
	// r.Recorder.Event(dgljob, corev1.EventTypeNormal, "Deleted", fmt.Sprintf("deleted %s %s", tp, obj.GetName()))
	return nil
}

func (r *DGLJobReconciler) createResource(ctx context.Context, dgljob *dglv1a1.DGLJob, obj client.Object) error {
	// TODO(ryantd): Recorder.Event will raise an error of nil pointer
	// tp := obj.GetObjectKind().GroupVersionKind().Kind
	if err := r.Create(ctx, obj); err != nil {
		// r.Recorder.Event(dgljob, corev1.EventTypeWarning, "Create", fmt.Sprintf("create failed %s %s", tp, obj.GetName()))
		return err
	}
	// r.Recorder.Event(dgljob, corev1.EventTypeNormal, "Created", fmt.Sprintf("created %s %s", tp, obj.GetName()))
	return nil

}

func (r *DGLJobReconciler) UpdateResource(ctx context.Context, dgljob *dglv1a1.DGLJob, obj client.Object) error {
	// TODO(ryantd): Recorder.Event will raise an error of nil pointer
	// tp := obj.GetObjectKind().GroupVersionKind().Kind
	if err := r.Update(ctx, obj); err != nil {
		// r.Recorder.Event(dgljob, corev1.EventTypeWarning, "Update", fmt.Sprintf("update failed %s %s", tp, obj.GetName()))
		return err
	}
	// r.Recorder.Event(dgljob, corev1.EventTypeNormal, "Updated", fmt.Sprintf("updated %s %s", tp, obj.GetName()))
	return nil

}

func indexerFunc(rawObj client.Object) []string {
	owner := metav1.GetControllerOf(rawObj)
	if owner == nil {
		return nil
	}
	if owner.APIVersion != apiGVStr || owner.Kind != dglv1a1.Kind {
		return nil
	}
	return []string{owner.Name}
}

func (r *DGLJobReconciler) SetupWithManager(mgr ctrl.Manager) error {
	// Pod indexer
	if err := mgr.GetFieldIndexer().
		IndexField(context.Background(), &corev1.Pod{}, jobOwnerKey, indexerFunc); err != nil {
		return err
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&dglv1a1.DGLJob{}).
		Owns(&corev1.Pod{}).
		Complete(r)
}

// getLauncherPod gets the launcher Pod controlled by this DGLJob
func (r *DGLJobReconciler) getLauncherPod(ctx context.Context, dgljob *dglv1a1.DGLJob) (*corev1.Pod, error) {
	var launcherPod corev1.Pod
	err := r.Get(
		ctx,
		types.NamespacedName{Name: dgljob.Name + launcherSuffix, Namespace: dgljob.Namespace},
		&launcherPod)
	if errors.IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		// If an error occurs during Get, we'll requeue the item so we can
		// attempt processing again later. This could have been caused by a
		// temporary network failure, or any other transient reason.
		return nil, err
	}

	return &launcherPod, nil
}

// getRunningPods gets the Pods controlled by this DGLJob
func (r *DGLJobReconciler) getRunningPods(ctx context.Context, dgljob *dglv1a1.DGLJob, replicaType dglv1a1.ReplicaType) (*corev1.PodList, error) {
	var podList corev1.PodList
	if err := r.List(
		ctx,
		&podList,
		client.InNamespace(dgljob.Namespace),
		client.MatchingFields{jobOwnerKey: dgljob.Name},
		client.MatchingLabels{dglv1a1.DGLReplicaType: string(replicaType)},
	); err != nil {
		return nil, err
	}
	return &podList, nil
}

// getOrCreateConfigMap gets the ConfigMap controlled by this DGLJob, or creates
// one if it doesn't exist.
func (r *DGLJobReconciler) getOrCreateConfigMap(dgljob *dglv1a1.DGLJob, workerReplicas int) (*corev1.ConfigMap, error) {
	ctx := context.Background()

	newCM := buildConfigMap(dgljob, workerReplicas)
	workerPods, err := r.getRunningPods(ctx, dgljob, dglv1a1.WorkerReplica)
	if err != nil {
		return nil, err
	}
	updateHostfileInConfigMap(&newCM, dgljob, workerPods)

	partitionerPods, err := r.getRunningPods(ctx, dgljob, dglv1a1.PartitionerReplica)
	if err != nil {
		return nil, err
	}
	updatePartfileInConfigMap(&newCM, dgljob, partitionerPods)

	launcherPods, err := r.getRunningPods(ctx, dgljob, dglv1a1.LauncherReplica)
	if err != nil {
		return nil, err
	}
	updateLeadfileInConfigMap(&newCM, dgljob, launcherPods)

	var cm corev1.ConfigMap
	err = r.Get(
		ctx,
		types.NamespacedName{Name: dgljob.Name + configSuffix, Namespace: dgljob.Namespace},
		&cm)
	if errors.IsNotFound(err) {
		cm = *newCM.DeepCopy()
		if err := ctrl.SetControllerReference(dgljob, &cm, r.Scheme); err != nil {
			r.Log.Error(err, "make reference failed of ConfigMap")
			return nil, nil
		}
		err := r.createResource(ctx, dgljob, &cm)
		if errors.IsConflict(err) {
			return nil, nil
		} else if err != nil {
			return nil, err
		}
		r.Log.Info("ConfigMap created", "ConfigMap name", cm.Name)
	} else if err != nil {
		return nil, err
	}

	// If the ConfigMap is changed, update it
	if !reflect.DeepEqual(cm.Data, newCM.Data) {
		cm.Data = newCM.Data
		err = r.UpdateResource(ctx, dgljob, &cm)
		if err != nil {
			return nil, err
		}
	}
	return &cm, nil
}

// getOrCreateLauncherServiceAccount gets the launcher ServiceAccount controlled
// by this DGLJob, or creates one if it doesn't exist.
func (r *DGLJobReconciler) getOrCreateLauncherServiceAccount(dgljob *dglv1a1.DGLJob) (*corev1.ServiceAccount, error) {
	return r.getOrCreateServiceAccount(dgljob, dgljob.Name+launcherSuffix)
}

// getOrCreatePartitionerServiceAccount gets the worker chief ServiceAccount controlled
// by this DGLJob, or creates one if it doesn't exist.
func (r *DGLJobReconciler) getOrCreatePartitionerServiceAccount(dgljob *dglv1a1.DGLJob) (*corev1.ServiceAccount, error) {
	return r.getOrCreateServiceAccount(dgljob, fmt.Sprintf("%s%s", dgljob.Name, partitionerSuffix))
}

// getOrCreateServiceAccount gets the launcher ServiceAccount controlled
// by this DGLJob, or creates one if it doesn't exist.
func (r *DGLJobReconciler) getOrCreateServiceAccount(dgljob *dglv1a1.DGLJob, name string) (*corev1.ServiceAccount, error) {
	ctx := context.Background()

	var sa corev1.ServiceAccount
	err := r.Get(
		ctx,
		types.NamespacedName{Name: name, Namespace: dgljob.Namespace},
		&sa)
	if errors.IsNotFound(err) {
		sa = buildServiceAccount(dgljob, name)
		if err := ctrl.SetControllerReference(dgljob, &sa, r.Scheme); err != nil {
			r.Log.Error(err, "make reference failed of ServiceAccount")
			return nil, nil
		}
		err := r.createResource(ctx, dgljob, &sa)
		if errors.IsConflict(err) {
			return nil, nil
		} else if err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}

	return &sa, nil
}

// getOrCreateLauncherRole gets the launcher Role controlled by this DGLJob.
func (r *DGLJobReconciler) getOrCreateLauncherRole(dgljob *dglv1a1.DGLJob, workerReplicas int) (*rbacv1.Role, error) {
	return r.getOrCreateRole(dgljob, workerReplicas, dgljob.Name+launcherSuffix, dglv1a1.LauncherReplica)
}

// getOrCreatePartitionerRole gets the launcher Role controlled by this DGLJob.
func (r *DGLJobReconciler) getOrCreatePartitionerRole(dgljob *dglv1a1.DGLJob, workerReplicas int) (*rbacv1.Role, error) {
	return r.getOrCreateRole(dgljob, workerReplicas, dgljob.Name+partitionerSuffix, dglv1a1.PartitionerReplica)
}

// getOrCreateRole gets the Role controlled by this DGLJob.
func (r *DGLJobReconciler) getOrCreateRole(dgljob *dglv1a1.DGLJob, workerReplicas int, name string, rType dglv1a1.ReplicaType) (*rbacv1.Role, error) {
	ctx := context.Background()

	var role rbacv1.Role
	err := r.Get(
		ctx,
		types.NamespacedName{Name: name, Namespace: dgljob.Namespace},
		&role)
	if errors.IsNotFound(err) {
		if rType == dglv1a1.LauncherReplica {
			role = buildRole(dgljob, workerReplicas, name)
		} else if rType == dglv1a1.PartitionerReplica {
			role = buildPartitionerRole(dgljob, name)
		}
		if err := ctrl.SetControllerReference(dgljob, &role, r.Scheme); err != nil {
			r.Log.Error(err, "make reference failed of Role")
			return nil, nil
		}
		err := r.createResource(ctx, dgljob, &role)
		if errors.IsConflict(err) {
			return nil, nil
		} else if err != nil {
			return nil, err
		}
		r.Log.Info("Role created", "Role name", role.Name)
	} else if err != nil {
		return nil, err
	}

	var newRole rbacv1.Role
	if rType == dglv1a1.LauncherReplica {
		newRole = buildRole(dgljob, workerReplicas, name)
	} else if rType == dglv1a1.PartitionerReplica {
		newRole = buildPartitionerRole(dgljob, name)
	}
	if !reflect.DeepEqual(role.Rules, newRole.Rules) {
		role.Rules = newRole.Rules
		err := r.UpdateResource(ctx, dgljob, &role)
		if errors.IsConflict(err) {
			return nil, nil
		} else if err != nil {
			return nil, err
		}
		r.Log.Info("Role updated", "Role name", role.Name)
	}

	return &role, nil
}

// getOrCreateLauncherRoleBinding gets the launcher RoleBinding controlledby this
// DGLJob, or creates one if it doesn't exist.
func (r *DGLJobReconciler) getOrCreateLauncherRoleBinding(dgljob *dglv1a1.DGLJob) (*rbacv1.RoleBinding, error) {
	return r.getOrCreateRoleBinding(dgljob, dgljob.Name+launcherSuffix)
}

// getOrCreatePartitionerRoleBinding gets the worker chief RoleBinding controlledby this
// DGLJob, or creates one if it doesn't exist.
func (r *DGLJobReconciler) getOrCreatePartitionerRoleBinding(dgljob *dglv1a1.DGLJob) (*rbacv1.RoleBinding, error) {
	return r.getOrCreateRoleBinding(dgljob, fmt.Sprintf("%s%s", dgljob.Name, partitionerSuffix))
}

// getOrCreateRoleBinding gets the RoleBinding controlledby this
// DGLJob, or creates one if it doesn't exist.
func (r *DGLJobReconciler) getOrCreateRoleBinding(dgljob *dglv1a1.DGLJob, name string) (*rbacv1.RoleBinding, error) {
	ctx := context.Background()

	var rb rbacv1.RoleBinding
	err := r.Get(
		ctx,
		types.NamespacedName{Name: name, Namespace: dgljob.Namespace},
		&rb)
	if errors.IsNotFound(err) {
		rb = buildRoleBinding(dgljob, name)
		if err := ctrl.SetControllerReference(dgljob, &rb, r.Scheme); err != nil {
			r.Log.Error(err, "make reference failed of RoleBinding")
			return nil, nil
		}
		err := r.createResource(ctx, dgljob, &rb)
		if errors.IsConflict(err) {
			return nil, nil
		} else if err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}

	return &rb, nil
}

// getOrCreateWorkers gets the worker Pod controlled by this DGLJob, or
// creates one if it doesn't exist.
func (r *DGLJobReconciler) getOrCreateWorkers(dgljob *dglv1a1.DGLJob) (*corev1.PodList, error) {
	var (
		workerPrefix   string = dgljob.Name + workerSuffix
		workerPods     corev1.PodList
		i              int = 0
		workerReplicas *int
	)
	if workerSpec, ok := dgljob.Spec.DGLReplicaSpecs[dglv1a1.WorkerReplica]; ok && workerSpec != nil {
		workerReplicas = workerSpec.Replicas
	} else {
		return &corev1.PodList{}, nil
	}

	for ; i < *workerReplicas; i++ {
		podName := fmt.Sprintf("%s-%d", workerPrefix, i)
		worker, err := r.getOrCreatePod(dgljob, dglv1a1.WorkerReplica, podName)
		if err != nil || worker == nil {
			continue
		}

		workerPods.Items = append(workerPods.Items, *worker)
	}

	return &workerPods, nil
}

// deleteWorkers delete the worker Pod controlled by this DGLJob
func (r *DGLJobReconciler) deleteWorkers(dgljob *dglv1a1.DGLJob) error {
	ctx := context.Background()
	var (
		workerPrefix   string = dgljob.Name + workerSuffix
		i              int    = 0
		workerReplicas *int
	)
	if workerSpec, ok := dgljob.Spec.DGLReplicaSpecs[dglv1a1.WorkerReplica]; ok && workerSpec != nil {
		workerReplicas = workerSpec.Replicas
	} else {
		return nil
	}

	for ; i < *workerReplicas; i++ {
		name := fmt.Sprintf("%s-%d", workerPrefix, i)
		var pod corev1.Pod
		err := r.Get(
			ctx,
			types.NamespacedName{Name: name, Namespace: dgljob.Namespace},
			&pod,
		)

		// If the worker Pod doesn't exist, we'll create it.
		if errors.IsNotFound(err) {
			continue
		}

		// If the worker pod is not running and cleanupPolicy is
		// set to CleanPodPolicyRunning, keep the pod.
		// Note that pending pod should still be removed under this
		// situation, since it may turn to running in the future.
		if *dgljob.Spec.CleanPodPolicy == dglv1a1.CleanPodPolicyRunning && !isPodRunning(&pod) && !isPodPending(&pod) {
			// Keep the worker pod
			continue
		}
		err = r.deleteResource(ctx, dgljob, &pod)
		if err != nil && !errors.IsNotFound(err) {
			r.Log.Error(err, fmt.Sprintf("Failed to delete pod[%s/%s]: %v", dgljob.Namespace, name, err))
			return err
		}
	}
	return nil
}

// getOrCreatePartitioners gets the partitioner Pods controlled by this DGLJob, or
// creates one if it doesn't exist.
func (r *DGLJobReconciler) getOrCreatePartitioners(dgljob *dglv1a1.DGLJob) (*corev1.PodList, error) {
	var (
		partitionerName string = dgljob.Name + partitionerSuffix
		partitionerPods corev1.PodList
	)

	partitioner, err := r.getOrCreatePod(dgljob, dglv1a1.PartitionerReplica, partitionerName)
	if err != nil {
		return nil, err
	}

	partitionerPods.Items = append(partitionerPods.Items, *partitioner)

	return &partitionerPods, nil
}

// createLauncher gets the launcher Pod controlled by this DGLJob
func (r *DGLJobReconciler) createLauncher(dgljob *dglv1a1.DGLJob) (*corev1.Pod, error) {
	return r.getOrCreatePod(dgljob, dglv1a1.LauncherReplica, dgljob.Name+launcherSuffix)
}

// getOrCreatePod gets the Pod controlled by this DGLJob
func (r *DGLJobReconciler) getOrCreatePod(dgljob *dglv1a1.DGLJob, rType dglv1a1.ReplicaType, podName string) (*corev1.Pod, error) {
	ctx := context.Background()

	var pod corev1.Pod
	err := r.Get(
		ctx,
		types.NamespacedName{Name: podName, Namespace: dgljob.Namespace},
		&pod,
	)

	if errors.IsNotFound(err) {
		if rType == dglv1a1.LauncherReplica {
			pod = buildLauncherPod(dgljob, podName, r.WatcherLoopImage, r.KubectlDownloadImage)
		} else if rType == dglv1a1.WorkerReplica || rType == dglv1a1.PartitionerReplica {
			pod = buildWorkerOrPartitionerPod(dgljob, podName, rType, r.KubectlDownloadImage)
		}
		if err := ctrl.SetControllerReference(dgljob, &pod, r.Scheme); err != nil {
			r.Log.Error(err, "make reference failed of Pod")
			return nil, err
		}
		if err := r.createResource(ctx, dgljob, &pod); err != nil {
			r.Log.Error(err, "create pod failed")
			return nil, err
		}
		r.Log.Info("Pod created", "Pod name", podName)
	}
	// If an error occurs during Get/Create, we'll requeue the item so we
	// can attempt processing again later. This could have been caused by a
	// temporary network failure, or any other transient reason.
	if err != nil && !errors.IsNotFound(err) {
		// r.Recorder.Eventf(dgljob, corev1.EventTypeWarning, dglv1a1.Failed, "worker pod created failed: %v", err)
		return nil, err
	}

	return &pod, nil
}

// newConfigMap creates a new ConfigMap containing configurations for an DGLJob
// resource. It also sets the appropriate OwnerReferences on the resource so
// handleObject can discover the DGLJob resource that 'owns' it.
func buildConfigMap(dgljob *dglv1a1.DGLJob, workerReplicas int) corev1.ConfigMap {
	kubexec := fmt.Sprintf(`#!/bin/sh
set -x
POD_NAME=$1; shift
%s/kubectl exec ${POD_NAME}`, kubectlMountPath)
	kubexec = fmt.Sprintf("%s -- /bin/sh -c \"$*\"", kubexec)

	return corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      dgljob.Name + configSuffix,
			Namespace: dgljob.Namespace,
			Labels: map[string]string{
				"app": dgljob.Name,
			},
		},
		Data: map[string]string{
			kubexecScriptName: kubexec,
		},
	}
}

// buildWorkerOrPartitionerPod creates a new worker or partitioner Pod
// for an DGLJob resource
func buildWorkerOrPartitionerPod(dgljob *dglv1a1.DGLJob, name string, rType dglv1a1.ReplicaType, KubectlDownloadImage string) corev1.Pod {
	labels := map[string]string{
		dglv1a1.DGLReplicaName: name,
		dglv1a1.DGLReplicaType: string(rType),
	}
	annotations := map[string]string{
		dglv1a1.DGLReplicaAnnotation: string(rType),
	}

	// partitioner reuse worker template
	podSpec := dgljob.Spec.DGLReplicaSpecs[dglv1a1.WorkerReplica].Template.DeepCopy()

	if len(podSpec.Labels) == 0 {
		podSpec.Labels = make(map[string]string)
	}
	for key, value := range labels {
		podSpec.Labels[key] = value
	}

	if len(podSpec.Annotations) == 0 {
		podSpec.Annotations = make(map[string]string)
	}
	for key, value := range annotations {
		podSpec.Annotations[key] = value
	}

	podSpec.Spec.RestartPolicy = "Never"

	if len(podSpec.Spec.Containers) == 0 {
		klog.Errorln("Worker pod does not have any containers in its spec")
		return corev1.Pod{}
	}
	container := podSpec.Spec.Containers[0]
	if len(container.Command) == 0 {
		container.Command = []string{"sleep"}
		container.Args = []string{"365d"}
	}

	container.Env = append(container.Env,
		corev1.EnvVar{
			Name:  kubeEnv,
			Value: "1",
		})
	container.VolumeMounts = append(
		container.VolumeMounts,
		corev1.VolumeMount{
			Name:      memoryVolumeName,
			MountPath: memoryMountPath,
		},
		corev1.VolumeMount{
			Name:      configVolumeName,
			MountPath: configMountPath,
		})
	container.Ports = append(
		container.Ports,
		corev1.ContainerPort{
			Name:          containerPortName,
			ContainerPort: dglv1a1.DGL_PORT,
			Protocol:      corev1.ProtocolTCP,
		})
	podSpec.Spec.Containers[0] = container

	shmSizeLimitInGB = container.Resources.Limits.Memory().Value() / 1000000000 / 2
	scriptsMode := int32(0555)
	hostfileMode := int32(0444)
	podSpec.Spec.Volumes = append(
		podSpec.Spec.Volumes,
		corev1.Volume{
			Name: memoryVolumeName,
			VolumeSource: corev1.VolumeSource{
				EmptyDir: &corev1.EmptyDirVolumeSource{
					Medium:    corev1.StorageMediumMemory,
					SizeLimit: resource.NewScaledQuantity(shmSizeLimitInGB, 9),
				},
			},
		},
		corev1.Volume{
			Name: configVolumeName,
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: dgljob.Name + configSuffix,
					},
					Items: []corev1.KeyToPath{
						{
							Key:  kubexecScriptName,
							Path: kubexecScriptName,
							Mode: &scriptsMode,
						},
						{
							Key:  hostfileName,
							Path: hostfileName,
							Mode: &hostfileMode,
						},
						{
							Key:  partfileName,
							Path: partfileName,
							Mode: &hostfileMode,
						},
						{
							Key:  leadfileName,
							Path: leadfileName,
							Mode: &hostfileMode,
						},
					},
				},
			},
		})

	if rType == dglv1a1.PartitionerReplica {
		kubectlDownloadContainer := corev1.Container{
			Name:            kubectlDownloadName,
			Image:           KubectlDownloadImage,
			ImagePullPolicy: corev1.PullAlways,
			VolumeMounts: []corev1.VolumeMount{
				{
					Name:      kubectlVolumeName,
					MountPath: kubectlMountPath,
				},
			},
		}

		podSpec.Spec.InitContainers = append(
			podSpec.Spec.InitContainers,
			kubectlDownloadContainer)

		mainContainer := dgljob.Spec.DGLReplicaSpecs[dglv1a1.LauncherReplica].Template.Spec.Containers[0]
		podSpec.Spec.Containers[0].Command = mainContainer.Command
		podSpec.Spec.Containers[0].Args = mainContainer.Args

		podSpec.Spec.Containers[0].Env = append(podSpec.Spec.Containers[0].Env,
			corev1.EnvVar{
				Name:  phaseEnv,
				Value: "Partitioner",
			})

		podSpec.Spec.Containers[0].VolumeMounts = append(
			podSpec.Spec.Containers[0].VolumeMounts,
			corev1.VolumeMount{
				Name:      kubectlVolumeName,
				MountPath: kubectlMountPath,
			})

		podSpec.Spec.ServiceAccountName = name
		podSpec.Spec.Volumes = append(
			podSpec.Spec.Volumes,
			corev1.Volume{
				Name: kubectlVolumeName,
				VolumeSource: corev1.VolumeSource{
					EmptyDir: &corev1.EmptyDirVolumeSource{},
				},
			})
	}

	return corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   dgljob.Namespace,
			Labels:      podSpec.Labels,
			Annotations: podSpec.Annotations,
		},
		Spec: podSpec.Spec,
	}
}

// buildLauncherPod creates a new launcher Pod for an DGLJob resource
func buildLauncherPod(dgljob *dglv1a1.DGLJob, name string, WatcherLoopImage string, KubectlDownloadImage string) corev1.Pod {
	labels := map[string]string{
		dglv1a1.DGLReplicaName: name,
		dglv1a1.DGLReplicaType: string(dglv1a1.LauncherReplica),
	}
	annotations := map[string]string{
		dglv1a1.DGLReplicaAnnotation: string(dglv1a1.LauncherReplica),
	}

	podSpec := dgljob.Spec.DGLReplicaSpecs[dglv1a1.LauncherReplica].Template.DeepCopy()

	if len(podSpec.Spec.Containers) == 0 {
		klog.Errorln("Launcher pod does not have any containers in its spec")
		return corev1.Pod{}
	}

	if len(podSpec.Labels) == 0 {
		podSpec.Labels = make(map[string]string)
	}
	for key, value := range labels {
		podSpec.Labels[key] = value
	}

	if len(podSpec.Annotations) == 0 {
		podSpec.Annotations = make(map[string]string)
	}
	for key, value := range annotations {
		podSpec.Annotations[key] = value
	}

	podSpec.Spec.RestartPolicy = "Never"

	if isPartitionModeDGLAPI(dgljob) {
		// InitContainers
		kubectlDownloadContainer := corev1.Container{
			Name:            kubectlDownloadName,
			Image:           KubectlDownloadImage,
			ImagePullPolicy: corev1.PullAlways,
			VolumeMounts: []corev1.VolumeMount{
				{
					Name:      kubectlVolumeName,
					MountPath: kubectlMountPath,
				},
			},
		}
		PartitionWatcherContainer := corev1.Container{
			Name:            partitionerWatcherName,
			Image:           WatcherLoopImage,
			ImagePullPolicy: corev1.PullAlways,
			Env: []corev1.EnvVar{
				{
					Name:  "NAMESPACE",
					Value: dgljob.Namespace,
				},
				{
					Name:  "WATCHERFILE",
					Value: fmt.Sprintf("%s/%s", configMountPath, partfileName),
				},
				{
					Name:  "WATCHERMODE",
					Value: "finished",
				},
			},
			VolumeMounts: []corev1.VolumeMount{
				{
					Name:      configVolumeName,
					MountPath: configMountPath,
				},
				{
					Name:      datasetVolumeName,
					MountPath: datasetMountPath,
				},
			},
			Resources: corev1.ResourceRequirements{
				Limits: corev1.ResourceList{
					corev1.ResourceCPU:              resource.MustParse(initContainerCpu),
					corev1.ResourceMemory:           resource.MustParse(initContainerMem),
					corev1.ResourceEphemeralStorage: resource.MustParse(initContainerEphStorage),
				},
				Requests: corev1.ResourceList{
					corev1.ResourceCPU:              resource.MustParse(initContainerCpu),
					corev1.ResourceMemory:           resource.MustParse(initContainerMem),
					corev1.ResourceEphemeralStorage: resource.MustParse(initContainerEphStorage),
				},
			},
		}

		WorkersWatcherContainer := corev1.Container{
			Name:            workerWatcherName,
			Image:           WatcherLoopImage,
			ImagePullPolicy: corev1.PullAlways,
			Env: []corev1.EnvVar{
				{
					Name:  "NAMESPACE",
					Value: dgljob.Namespace,
				},
				{
					Name:  "WATCHERFILE",
					Value: fmt.Sprintf("%s/%s", configMountPath, hostfileName),
				},
				{
					Name:  "WATCHERMODE",
					Value: "ready",
				},
			},
			VolumeMounts: []corev1.VolumeMount{
				{
					Name:      configVolumeName,
					MountPath: configMountPath,
				},
			},
			Resources: corev1.ResourceRequirements{
				Limits: corev1.ResourceList{
					corev1.ResourceCPU:              resource.MustParse(initContainerCpu),
					corev1.ResourceMemory:           resource.MustParse(initContainerMem),
					corev1.ResourceEphemeralStorage: resource.MustParse(initContainerEphStorage),
				},
				Requests: corev1.ResourceList{
					corev1.ResourceCPU:              resource.MustParse(initContainerCpu),
					corev1.ResourceMemory:           resource.MustParse(initContainerMem),
					corev1.ResourceEphemeralStorage: resource.MustParse(initContainerEphStorage),
				},
			},
		}
		podSpec.Spec.InitContainers = append(
			podSpec.Spec.InitContainers,
			kubectlDownloadContainer,
			PartitionWatcherContainer,
			WorkersWatcherContainer)

		// Main Containers
		container := podSpec.Spec.Containers[0]
		container.Env = append(container.Env,
			corev1.EnvVar{
				Name:  kubexecPathEnv,
				Value: fmt.Sprintf("%s/%s", configMountPath, kubexecScriptName),
			},
			corev1.EnvVar{
				Name:  hostfilePathEnv,
				Value: fmt.Sprintf("%s/%s", configMountPath, hostfileName),
			},
			corev1.EnvVar{
				Name:  kubectlPathEnv,
				Value: fmt.Sprintf("%s/%s", kubectlMountPath, kubectlName),
			},
			corev1.EnvVar{
				Name:  kubeEnv,
				Value: "1",
			})

		container.VolumeMounts = append(container.VolumeMounts,
			corev1.VolumeMount{
				Name:      kubectlVolumeName,
				MountPath: kubectlMountPath,
			},
			corev1.VolumeMount{
				Name:      configVolumeName,
				MountPath: configMountPath,
			},
			corev1.VolumeMount{
				Name:      datasetVolumeName,
				MountPath: datasetMountPath,
			})
		if container.Resources.Size() == 0 {
			container.Resources = corev1.ResourceRequirements{
				Limits: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse(defaultLauncherContainerCpu),
					corev1.ResourceMemory: resource.MustParse(defaultLauncherContainerMem),
				},
				Requests: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse(defaultLauncherContainerCpu),
					corev1.ResourceMemory: resource.MustParse(defaultLauncherContainerMem),
				},
			}
		}
		podSpec.Spec.Containers[0] = container
		podSpec.Spec.ServiceAccountName = name

		scriptsMode := int32(0555)
		hostfileMode := int32(0444)
		partfileMode := int32(0444)
		podSpec.Spec.Volumes = append(podSpec.Spec.Volumes,
			corev1.Volume{
				Name: kubectlVolumeName,
				VolumeSource: corev1.VolumeSource{
					EmptyDir: &corev1.EmptyDirVolumeSource{},
				},
			},
			corev1.Volume{
				Name: datasetVolumeName,
				VolumeSource: corev1.VolumeSource{
					EmptyDir: &corev1.EmptyDirVolumeSource{},
				},
			},
			corev1.Volume{
				Name: configVolumeName,
				VolumeSource: corev1.VolumeSource{
					ConfigMap: &corev1.ConfigMapVolumeSource{
						LocalObjectReference: corev1.LocalObjectReference{
							Name: dgljob.Name + configSuffix,
						},
						Items: []corev1.KeyToPath{
							{
								Key:  kubexecScriptName,
								Path: kubexecScriptName,
								Mode: &scriptsMode,
							},
							{
								Key:  hostfileName,
								Path: hostfileName,
								Mode: &hostfileMode,
							},
							{
								Key:  partfileName,
								Path: partfileName,
								Mode: &partfileMode,
							},
						},
					},
				},
			})
	} else if isPartitionModeSkip(dgljob) {
		podSpec.Spec.Containers[0].Env = append(podSpec.Spec.Containers[0].Env,
			corev1.EnvVar{
				Name:  phaseEnv,
				Value: "Launcher_Workload",
			})

		if podSpec.Spec.Containers[0].Resources.Size() == 0 {
			podSpec.Spec.Containers[0].Resources = corev1.ResourceRequirements{
				Limits: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse(defaultLauncherContainerCpu),
					corev1.ResourceMemory: resource.MustParse(defaultLauncherContainerMem),
				},
				Requests: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse(defaultLauncherContainerCpu),
					corev1.ResourceMemory: resource.MustParse(defaultLauncherContainerMem),
				},
			}
		}
	}

	return corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   dgljob.Namespace,
			Labels:      podSpec.Labels,
			Annotations: podSpec.Annotations,
		},
		Spec: podSpec.Spec,
	}
}

// buildServiceAccount creates a new launcher ServiceAccount for an DGLJob resource
func buildServiceAccount(dgljob *dglv1a1.DGLJob, name string) corev1.ServiceAccount {
	return corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: dgljob.Namespace,
			Labels: map[string]string{
				"app": dgljob.Name,
			},
		},
	}
}

// buildRole creates a new Role for an DGLJob resource
func buildRole(dgljob *dglv1a1.DGLJob, workerReplicas int, name string) rbacv1.Role {
	var podNames []string
	for i := 0; i < int(workerReplicas); i++ {
		podNames = append(podNames, fmt.Sprintf("%s%s-%d", dgljob.Name, workerSuffix, i))
	}
	return rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: dgljob.Namespace,
			Labels: map[string]string{
				"app": dgljob.Name,
			},
		},
		Rules: []rbacv1.PolicyRule{
			{
				Verbs:     []string{"get", "list", "watch"},
				APIGroups: []string{""},
				Resources: []string{"pods"},
			},
			{
				Verbs:         []string{"create"},
				APIGroups:     []string{""},
				Resources:     []string{"pods/exec"},
				ResourceNames: podNames,
			},
		},
	}
}

// buildPartitionerRole creates a new Role for an DGLJob resource of partitioner
func buildPartitionerRole(dgljob *dglv1a1.DGLJob, name string) rbacv1.Role {
	var podNames []string
	podNames = append(podNames, dgljob.Name+launcherSuffix)
	return rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: dgljob.Namespace,
			Labels: map[string]string{
				"app": dgljob.Name,
			},
		},
		Rules: []rbacv1.PolicyRule{
			{
				Verbs:     []string{"get", "list", "watch"},
				APIGroups: []string{""},
				Resources: []string{"pods"},
			},
			{
				Verbs:         []string{"create"},
				APIGroups:     []string{""},
				Resources:     []string{"pods/exec"},
				ResourceNames: podNames,
			},
		},
	}
}

// buildRoleBinding creates a new RoleBinding for an DGLJob resource
func buildRoleBinding(dgljob *dglv1a1.DGLJob, name string) rbacv1.RoleBinding {
	return rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: dgljob.Namespace,
			Labels: map[string]string{
				"app": dgljob.Name,
			},
		},
		Subjects: []rbacv1.Subject{
			{
				Kind:      rbacv1.ServiceAccountKind,
				Name:      name,
				Namespace: dgljob.Namespace,
			},
		},
		RoleRef: rbacv1.RoleRef{
			APIGroup: rbacv1.GroupName,
			Kind:     "Role",
			Name:     name,
		},
	}
}

// updateHostfileInConfigMap updates the ConfigMap if the content of `hostfile` changes.
func updateHostfileInConfigMap(configMap *corev1.ConfigMap, dgljob *dglv1a1.DGLJob, runningPods *corev1.PodList) {
	slots := 1
	if dgljob.Spec.SlotsPerWorker != nil {
		slots = int(*dgljob.Spec.SlotsPerWorker)
	}

	pods := runningPods.Items
	sort.Slice(pods, func(i, j int) bool {
		return pods[i].Name < pods[j].Name
	})

	var hostfileBuffer bytes.Buffer
	for i, p := range pods {
		hostfileBuffer.WriteString(fmt.Sprintf("%s %d %s%s-%d slots=%d\n", p.Status.PodIP, dglv1a1.DGL_PORT, dgljob.Name, workerSuffix, i, slots))
	}

	oldHostfile, exist := configMap.Data[hostfileName]
	if exist && oldHostfile == hostfileBuffer.String() {
		return
	}
	configMap.Data[hostfileName] = hostfileBuffer.String()
}

// updatePartfileInConfigMap updates the ConfigMap if the content of `partfile` changes.
func updatePartfileInConfigMap(configMap *corev1.ConfigMap, dgljob *dglv1a1.DGLJob, runningPods *corev1.PodList) {
	pods := runningPods.Items

	var partfileBuffer bytes.Buffer
	for _, p := range pods {
		partfileBuffer.WriteString(fmt.Sprintf("%s %d %s%s\n", p.Status.PodIP, dglv1a1.DGL_PORT, dgljob.Name, partitionerSuffix))
	}

	oldPartfile, exist := configMap.Data[partfileName]
	if exist && oldPartfile == partfileBuffer.String() {
		return
	}
	configMap.Data[partfileName] = partfileBuffer.String()
}

// updateLeadfileInConfigMap updates the ConfigMap if the content of `leadfile` changes.
func updateLeadfileInConfigMap(configMap *corev1.ConfigMap, dgljob *dglv1a1.DGLJob, runningPods *corev1.PodList) {
	pods := runningPods.Items

	var leadfileBuffer bytes.Buffer
	for _, p := range pods {
		leadfileBuffer.WriteString(fmt.Sprintf("%s %d %s%s\n", p.Status.PodIP, dglv1a1.DGL_PORT, dgljob.Name, launcherSuffix))
	}

	oldLeadfile, exist := configMap.Data[leadfileName]
	if exist && oldLeadfile == leadfileBuffer.String() {
		return
	}
	configMap.Data[leadfileName] = leadfileBuffer.String()
}

func genJobPhase(dgljob *dglv1a1.DGLJob) dglv1a1.JobPhase {
	if dgljob.Spec.DGLReplicaSpecs[dglv1a1.LauncherReplica] == nil ||
		dgljob.Spec.DGLReplicaSpecs[dglv1a1.LauncherReplica].Replicas == nil ||
		dgljob.Spec.DGLReplicaSpecs[dglv1a1.WorkerReplica] == nil ||
		dgljob.Spec.DGLReplicaSpecs[dglv1a1.WorkerReplica].Replicas == nil ||
		dgljob.Spec.DGLReplicaSpecs[dglv1a1.PartitionerReplica] == nil ||
		dgljob.Spec.DGLReplicaSpecs[dglv1a1.PartitionerReplica].Replicas == nil ||
		dgljob.Status.ReplicaStatuses[dglv1a1.LauncherReplica] == nil ||
		dgljob.Status.ReplicaStatuses[dglv1a1.WorkerReplica] == nil ||
		dgljob.Status.ReplicaStatuses[dglv1a1.PartitionerReplica] == nil {
		return dglv1a1.Pending
	}

	if dgljob.Status.Phase == dglv1a1.Completed {
		return dglv1a1.Completed
	} else if dgljob.Status.Phase == dglv1a1.Failed {
		return dglv1a1.Failed
	} else if *dgljob.Spec.DGLReplicaSpecs[dglv1a1.PartitionerReplica].Replicas == dgljob.Status.ReplicaStatuses[dglv1a1.PartitionerReplica].Running {
		return dglv1a1.Partitioning
	} else if *dgljob.Spec.DGLReplicaSpecs[dglv1a1.PartitionerReplica].Replicas == dgljob.Status.ReplicaStatuses[dglv1a1.PartitionerReplica].Succeeded &&
		dgljob.Status.ReplicaStatuses[dglv1a1.WorkerReplica].Running == 0 {
		return dglv1a1.Partitioned
	} else if *dgljob.Spec.DGLReplicaSpecs[dglv1a1.LauncherReplica].Replicas == dgljob.Status.ReplicaStatuses[dglv1a1.LauncherReplica].Running &&
		*dgljob.Spec.DGLReplicaSpecs[dglv1a1.WorkerReplica].Replicas == dgljob.Status.ReplicaStatuses[dglv1a1.WorkerReplica].Running {
		return dglv1a1.Training
	} else if dgljob.Status.ReplicaStatuses[dglv1a1.LauncherReplica].Failed > 0 ||
		dgljob.Status.ReplicaStatuses[dglv1a1.WorkerReplica].Failed > 0 ||
		dgljob.Status.ReplicaStatuses[dglv1a1.PartitionerReplica].Failed > 0 {
		return dglv1a1.Failed
	} else if *dgljob.Spec.DGLReplicaSpecs[dglv1a1.LauncherReplica].Replicas == dgljob.Status.ReplicaStatuses[dglv1a1.LauncherReplica].Succeeded {
		return dglv1a1.Completed
	} else if dgljob.Status.ReplicaStatuses[dglv1a1.LauncherReplica].Pending > 0 ||
		dgljob.Status.ReplicaStatuses[dglv1a1.WorkerReplica].Pending > 0 ||
		dgljob.Status.ReplicaStatuses[dglv1a1.PartitionerReplica].Pending > 0 {
		return dglv1a1.Starting
	}

	return dglv1a1.Starting
}

func isPodRealRuning(pod *corev1.Pod) bool {
	if pod.Status.Phase != corev1.PodRunning {
		return false
	}
	for i := range pod.Status.InitContainerStatuses {
		container := pod.Status.InitContainerStatuses[i]
		if !container.Ready {
			return false
		}
	}
	for i := range pod.Status.ContainerStatuses {
		container := pod.Status.ContainerStatuses[i]
		if !container.Ready || container.State.Running == nil {
			return false
		}
	}
	return true
}

func isJobFinished(status dglv1a1.DGLJobStatus) bool {
	return isJobSucceeded(status) || isJobFailed(status)
}

func isJobSucceeded(status dglv1a1.DGLJobStatus) bool {
	return status.Phase == dglv1a1.Completed
}

func isJobFailed(status dglv1a1.DGLJobStatus) bool {
	return status.Phase == dglv1a1.Failed
}

func isJobEvicted(status dglv1a1.DGLJobStatus) bool {
	return status.Phase == dglv1a1.Evicted
}

func isPodFinished(j *corev1.Pod) bool {
	return isPodSucceeded(j) || isPodFailed(j)
}

func isPodFailed(p *corev1.Pod) bool {
	return p.Status.Phase == corev1.PodFailed
}

func isPodSucceeded(p *corev1.Pod) bool {
	return p.Status.Phase == corev1.PodSucceeded
}

func isPodRunning(p *corev1.Pod) bool {
	return p.Status.Phase == corev1.PodRunning
}

func isPodPending(p *corev1.Pod) bool {
	return p.Status.Phase == corev1.PodPending
}

func isCleanUpPods(cleanPodPolicy *dglv1a1.CleanPodPolicy) bool {
	if *cleanPodPolicy == dglv1a1.CleanPodPolicyAll || *cleanPodPolicy == dglv1a1.CleanPodPolicyRunning {
		return true
	}
	return false
}

func isPartitionModeDGLAPI(dgljob *dglv1a1.DGLJob) bool {
	return *dgljob.Spec.PartitionMode == dglv1a1.PartitionModeDGLAPI
}

func isPartitionModeSkip(dgljob *dglv1a1.DGLJob) bool {
	return *dgljob.Spec.PartitionMode == dglv1a1.PartitionModeSkip
}

func initializeDGLJobStatus(dgljob *dglv1a1.DGLJob, rType dglv1a1.ReplicaType) {
	if rType == dglv1a1.LauncherReplica {
		dgljob.Status.ReplicaStatuses[dglv1a1.LauncherReplica] = &dglv1a1.ReplicaStatus{}
	} else if rType == dglv1a1.WorkerReplica {
		dgljob.Status.ReplicaStatuses[dglv1a1.WorkerReplica] = &dglv1a1.ReplicaStatus{}
	}
}
