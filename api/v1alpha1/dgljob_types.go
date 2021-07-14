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

package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

const (
	Kind     = "DGLJob"
	DGL_PORT = 30050
	// Annotation key
	DGLReplicaAnnotation = "dgl-replica"
	// Label key
	DGLReplicaName = "dgl-replica-name"
	DGLReplicaType = "dgl-replica-type"
)

// JobPhase defines the phase of the job.
type JobPhase string

const (
	Starting     JobPhase = "Starting"
	Pending      JobPhase = "Pending"
	Partitioning JobPhase = "Partitioning"
	Partitioned  JobPhase = "Partitioned"
	Training     JobPhase = "Training"
	Completed    JobPhase = "Completed"
	Failed       JobPhase = "Failed"
	Evicted      JobPhase = "Evicted"
	Succeed      JobPhase = "Succeed"
)

// PartitionMode describes how to deal with the partitioning
type PartitionMode string

const (
	// PartitionModeDGLAPI will partition standalone
	PartitionModeDGLAPI PartitionMode = "DGL-API"
	// PartitionModeParMETIS will partition in a fully distributed manner
	PartitionModeParMETIS PartitionMode = "ParMETIS"
	// PartitionModeNone will skip the partitioning
	PartitionModeNone PartitionMode = "Skip"
)

// CleanPodPolicy describes how to deal with pods when the job is finished
type CleanPodPolicy string

const (
	// CleanPodPolicyAll policy will clean all pods when job finished
	CleanPodPolicyAll CleanPodPolicy = "All"
	// CleanPodPolicyRunning policy will clean running pods when job finished
	CleanPodPolicyRunning CleanPodPolicy = "Running"
	// CleanPodPolicyNever policy None
	CleanPodPolicyNone CleanPodPolicy = "None"
)

type ReplicaType string

const (
	LauncherReplica    ReplicaType = "Launcher"
	WorkerReplica      ReplicaType = "Worker"
	PartitionerReplica ReplicaType = "Partitioner"
)

// ReplicaSpec is a description of the replica
type ReplicaSpec struct {
	// Replicas replica
	Replicas *int `json:"replicas,omitempty"`

	// Template is the object that describes the pod that
	// will be created for this replica
	Template corev1.PodTemplateSpec `json:"template"`
}

// ReplicaStatus represents the current observed state of replica
type ReplicaStatus struct {
	// The number of actively running pods.
	Running int `json:"running,omitempty"`
	// The number of pending pods.
	Pending int `json:"pending,omitempty"`
	// The number of starting pods.
	Starting int `json:"starting,omitempty"`
	// The number of pods which reached phase Succeeded.
	Succeeded int `json:"succeeded,omitempty"`
	// The number of pods which reached phase Failed.
	Failed int `json:"failed,omitempty"`
	// Replica ready string
	Ready string `json:"ready,omitempty"`
}

// DGLJobSpec defines the desired state of DGLJob
type DGLJobSpec struct {
	// INSERT ADDITIONAL SPEC FIELDS - desired state of cluster
	// Important: Run "make" to regenerate code after modifying this file

	SlotsPerWorker *int `json:"slotsPerWorker,omitempty"`

	// PartitionMode defines the policy how to partition. Defaults to DGL-API.
	// +kubebuilder:default:=DGL-API
	PartitionMode *PartitionMode `json:"partitionMode,omitempty"`

	// CleanPodPolicy defines the policy that whether to kill pods after the job completes. Defaults to None.
	// +kubebuilder:default:=Running
	CleanPodPolicy *CleanPodPolicy `json:"cleanPodPolicy,omitempty"`

	// DGLReplicaSpecs describes the spec of launcher or worker base on pod template
	DGLReplicaSpecs map[ReplicaType]*ReplicaSpec `json:"dglReplicaSpecs"`
}

// DGLJobStatus defines the observed state of DGLJob
type DGLJobStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "make" to regenerate code after modifying this file

	Phase JobPhase `json:"phase,omitempty"`

	// ReplicaStatuses is map of ReplicaType and ReplicaStatus,
	// specifies the status of each replica.
	ReplicaStatuses map[ReplicaType]*ReplicaStatus `json:"replicaStatuses"`

	// Represents time when the job was acknowledged by the job controller.
	// It is not guaranteed to be set in happens-before order across separate operations.
	// It is represented in RFC3339 form and is in UTC.
	StartTime *metav1.Time `json:"startTime,omitempty"`

	// Represents time when the job was completed. It is not guaranteed to
	// be set in happens-before order across separate operations.
	// It is represented in RFC3339 form and is in UTC.
	CompletionTime *metav1.Time `json:"completionTime,omitempty"`

	// Represents last time when the job was reconciled. It is not guaranteed to
	// be set in happens-before order across separate operations.
	// It is represented in RFC3339 form and is in UTC.
	LastReconcileTime *metav1.Time `json:"lastReconcileTime,omitempty"`
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status

// DGLJob is the Schema for the dgljobs API
type DGLJob struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   DGLJobSpec   `json:"spec,omitempty"`
	Status DGLJobStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

// DGLJobList contains a list of DGLJob
type DGLJobList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []DGLJob `json:"items"`
}

func init() {
	SchemeBuilder.Register(&DGLJob{}, &DGLJobList{})
}
