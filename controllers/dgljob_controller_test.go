/*


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
	"context"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	dglv1a1 "github.com/Qihoo360/dgl-operator/api/v1alpha1"
)

var _ = Describe("DGLJob controller", func() {

	const (
		jobName      = "dgl-test-job"
		jobNamespace = "dgl-operator"
		jobImage     = "dgloperator/graphsage:v0.1.0"

		timeout  = time.Second * 120
		interval = time.Millisecond * 250
	)

	var (
		zeroReplica           = 0
		partitionerReplica    = 1
		launcherReplica       = 1
		workerReplica         = 2
		cleanPodPolicyRunning = dglv1a1.CleanPodPolicyRunning
	)

	workerPodSpec := corev1.PodTemplateSpec{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  jobName,
					Image: jobImage,
					Resources: corev1.ResourceRequirements{
						Limits: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("10"),
							corev1.ResourceMemory: resource.MustParse("20Gi"),
						},
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("10"),
							corev1.ResourceMemory: resource.MustParse("20Gi"),
						},
					},
				},
			},
		},
	}

	launcherPodSpec := corev1.PodTemplateSpec{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:    jobName,
					Image:   jobImage,
					Command: []string{"dglrun"},
					Args: []string{
						"--graph-name",
						"graphsage",
						"--partition-entry-point",
						"code/load_and_partition_graph.py",
						"--num-partitions",
						"2",
						"--balance-train",
						"--balance-edges",
						"--train-entry-point",
						"code/train_dist.py",
						"--num-epochs",
						"1",
						"--batch-size",
						"1000",
						"--num-trainers",
						"1",
						"--num-samplers",
						"4",
						"--num-servers",
						"1",
					},
				},
			},
		},
	}

	graphsageTestJob := dglv1a1.DGLJob{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "qihoo.net/v1alpha1",
			Kind:       "DGLJob",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName,
			Namespace: jobNamespace,
		},
		Spec: dglv1a1.DGLJobSpec{
			CleanPodPolicy: &cleanPodPolicyRunning,
			DGLReplicaSpecs: map[dglv1a1.ReplicaType]*dglv1a1.ReplicaSpec{
				dglv1a1.LauncherReplica: {
					Replicas: &launcherReplica,
					Template: launcherPodSpec,
				},
				dglv1a1.WorkerReplica: {
					Replicas: &workerReplica,
					Template: workerPodSpec,
				},
			},
		},
	}

	Context("When tracking DGLJob Status", func() {
		It("Should have a proper replica status when the job is partitioning, partitioned, training, or completed", func() {
			By("By creating a new DGLJob")
			dglJob := graphsageTestJob.DeepCopy()
			ctx := context.Background()
			jobKey := types.NamespacedName{
				Name:      jobName,
				Namespace: jobNamespace,
			}
			dglJobCreated := &dglv1a1.DGLJob{}

			initStatusCheck := func() func() bool {
				return func() bool {
					if err := k8sClient.Get(ctx, jobKey, dglJobCreated); err != nil {
						return false
					}
					return true
				}
			}

			partitioningStatusCheck := func() func() bool {
				return func() bool {
					if dglJobCreated.Status.Phase == dglv1a1.Partitioning {
						if dglJobCreated.Status.ReplicaStatuses[dglv1a1.PartitionerReplica].Running == partitionerReplica &&
							dglJobCreated.Status.ReplicaStatuses[dglv1a1.PartitionerReplica].Succeeded == zeroReplica &&
							dglJobCreated.Status.ReplicaStatuses[dglv1a1.WorkerReplica].Running == zeroReplica &&
							dglJobCreated.Status.ReplicaStatuses[dglv1a1.LauncherReplica].Running == launcherReplica {
							return true
						}
					}
					return false
				}
			}

			partitionedStatusCheck := func() func() bool {
				return func() bool {
					if dglJobCreated.Status.Phase == dglv1a1.Partitioned {
						if dglJobCreated.Status.ReplicaStatuses[dglv1a1.PartitionerReplica].Running == zeroReplica &&
							dglJobCreated.Status.ReplicaStatuses[dglv1a1.PartitionerReplica].Succeeded == partitionerReplica &&
							dglJobCreated.Status.ReplicaStatuses[dglv1a1.WorkerReplica].Running == zeroReplica &&
							dglJobCreated.Status.ReplicaStatuses[dglv1a1.LauncherReplica].Running == launcherReplica {
							return true
						}
					}
					return false
				}
			}

			trainingStatusCheck := func() func() bool {
				return func() bool {
					if dglJobCreated.Status.Phase == dglv1a1.Training {
						if dglJobCreated.Status.ReplicaStatuses[dglv1a1.PartitionerReplica].Running == zeroReplica &&
							dglJobCreated.Status.ReplicaStatuses[dglv1a1.PartitionerReplica].Succeeded == partitionerReplica &&
							dglJobCreated.Status.ReplicaStatuses[dglv1a1.WorkerReplica].Running == workerReplica &&
							dglJobCreated.Status.ReplicaStatuses[dglv1a1.LauncherReplica].Running == launcherReplica {
							return true
						}
					}
					return false
				}
			}

			completedStatusCheck := func() func() bool {
				return func() bool {
					if dglJobCreated.Status.Phase == dglv1a1.Completed {
						if dglJobCreated.Status.ReplicaStatuses[dglv1a1.PartitionerReplica].Running == zeroReplica &&
							dglJobCreated.Status.ReplicaStatuses[dglv1a1.PartitionerReplica].Succeeded == partitionerReplica &&
							dglJobCreated.Status.ReplicaStatuses[dglv1a1.WorkerReplica].Running == zeroReplica &&
							dglJobCreated.Status.ReplicaStatuses[dglv1a1.LauncherReplica].Running == zeroReplica &&
							dglJobCreated.Status.ReplicaStatuses[dglv1a1.LauncherReplica].Succeeded == launcherReplica {
							return true
						}
					}
					return false
				}
			}

			Expect(k8sClient.Create(ctx, dglJob)).Should(Succeed())
			Eventually(initStatusCheck(), timeout, interval).Should(BeTrue())
			Eventually(partitioningStatusCheck(), timeout, interval).Should(BeTrue())
			Eventually(partitionedStatusCheck(), timeout, interval).Should(BeTrue())
			Eventually(trainingStatusCheck(), timeout, interval).Should(BeTrue())
			Eventually(completedStatusCheck(), timeout, interval).Should(BeTrue())
		})
	})
})
