// Copyright 2020 The Kubeflow Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package watcher_loop

import (
	"fmt"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	coreinformers "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog"
)

// WatcherLoopController
type WatcherLoopController struct {
	namespace string
	// kubeClient is a standard kubernetes clientset.
	kubeClient kubernetes.Interface

	podLister corelisters.PodLister
	podSynced cache.InformerSynced

	watcherMode string

	// queue is a rate limited work queue. This is used to queue work to be
	// processed instead of performing it as soon as a change happens. This
	// means we can ensure we only process a fixed amount of resources at a
	// time, and makes it easy to ensure we are never processing the same item
	// simultaneously in two different workers.
	queue workqueue.RateLimitingInterface

	lock        sync.Mutex
	watchedPods map[string]struct{}
}

// NewWatcherLoopController
func NewWatcherLoopController(
	ns string,
	kubeClient kubernetes.Interface,
	podInformer coreinformers.PodInformer,
	pods []string,
	watcherMode string,
) *WatcherLoopController {

	controller := &WatcherLoopController{
		namespace:   ns,
		kubeClient:  kubeClient,
		podLister:   podInformer.Lister(),
		podSynced:   podInformer.Informer().HasSynced,
		queue:       workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "mpi-operator-watcher-loop"),
		watchedPods: make(map[string]struct{}),
		watcherMode: watcherMode,
	}

	controller.lock.Lock()
	for _, pn := range pods {
		controller.watchedPods[pn] = struct{}{}
	}
	controller.lock.Unlock()

	klog.Infof("watched pods: %v", pods)
	klog.Info("Setting up event handlers")
	// Set up an event handler for when MPIJob resources change.
	podInformer.Informer().AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: func(obj interface{}) bool {
			pod, ok := obj.(*corev1.Pod)
			if !ok {
				return false
			}
			if _, ok := controller.watchedPods[pod.Name]; ok {
				return true
			}
			return false
		},
		Handler: cache.ResourceEventHandlerFuncs{
			UpdateFunc: func(old, new interface{}) {
				controller.enqueue(new)
			},
		},
	})

	return controller
}

// Run will set up the event handlers for types we are interested in, as well
// as syncing informer caches and starting workers. It will block until stopCh
// is closed, at which point it will shutdown the work queue and wait for
// workers to finish processing their current work items.
func (c *WatcherLoopController) Run(threadiness int, stopCh <-chan struct{}) error {
	defer runtime.HandleCrash()
	defer c.queue.ShutDown()

	// Start the informer factories to begin populating the informer caches.
	klog.Info("Starting watcher-loop controller")

	// Wait for the caches to be synced before starting workers.
	klog.Info("Waiting for informer caches to sync")
	if ok := cache.WaitForCacheSync(stopCh, c.podSynced); !ok {
		return fmt.Errorf("failed to wait for caches to sync")
	}
	for name := range c.watchedPods {
		pod, err := c.podLister.Pods(c.namespace).Get(name)
		if err != nil {
			continue
		}
		if (c.watcherMode == "ready" && pod.Status.Phase == corev1.PodRunning) ||
			(c.watcherMode == "finished" && pod.Status.Phase == corev1.PodSucceeded) {
			c.lock.Lock()
			delete(c.watchedPods, pod.Name)
			c.lock.Unlock()
		}
	}
	klog.Info("Starting workers")
	// Launch workers to process MPIJob resources.
	for i := 0; i < threadiness; i++ {
		go wait.Until(c.runWorker, 100*time.Millisecond, stopCh)
	}

	klog.Info("Started workers")
	ticker := time.NewTicker(500 * time.Millisecond)
	for {
		select {
		case <-stopCh:
			return nil
		case <-ticker.C:
			if len(c.watchedPods) == 0 {
				klog.Info("Shutting down workers")
				return nil
			}
			break
		}
	}
}

// runWorker is a long-running function that will continually call the
// processNextWorkItem function in order to read and process a message on the
// work queue.
func (c *WatcherLoopController) runWorker() {
	for c.processNextWorkItem() {
	}
}

// processNextWorkItem will read a single work item off the work queue and
// attempt to process it, by calling the syncHandler.
func (c *WatcherLoopController) processNextWorkItem() bool {
	obj, shutdown := c.queue.Get()

	if shutdown {
		return false
	}

	// We wrap this block in a func so we can defer c.queue.Done.
	err := func(obj interface{}) error {
		// We call Done here so the work queue knows we have finished
		// processing this item. We also must remember to call Forget if we
		// do not want this work item being re-queued. For example, we do
		// not call Forget if a transient error occurs, instead the item is
		// put back on the work queue and attempted again after a back-off
		// period.
		defer c.queue.Done(obj)
		var key string
		var ok bool
		// We expect strings to come off the work queue. These are of the
		// form namespace/name. We do this as the delayed nature of the
		// work queue means the items in the informer cache may actually be
		// more up to date that when the item was initially put onto the
		// work queue.
		if key, ok = obj.(string); !ok {
			// As the item in the work queue is actually invalid, we call
			// Forget here else we'd go into a loop of attempting to
			// process a work item that is invalid.
			c.queue.Forget(obj)
			runtime.HandleError(fmt.Errorf("expected string in workqueue but got %#v", obj))
			return nil
		}
		// Run the syncHandler, passing it the namespace/name string of the
		// MPIJob resource to be synced.
		if err := c.syncHandler(key); err != nil {
			c.queue.AddRateLimited(key)
			return fmt.Errorf("error syncing '%s': %s", key, err.Error())
		}
		// Finally, if no error occurs we Forget this item so it does not
		// get queued again until another change happens.
		c.queue.Forget(obj)
		klog.Infof("Successfully synced '%s'", key)
		return nil
	}(obj)

	if err != nil {
		runtime.HandleError(err)
		return true
	}
	return true
}

// syncHandler compares the actual state with the desired, and attempts to
// converge the two. It then updates the Status block of the MPIJob resource
// with the current status of the resource.
func (c *WatcherLoopController) syncHandler(key string) error {
	startTime := time.Now()
	defer func() {
		klog.Infof("Finished syncing job %q (%v)", key, time.Since(startTime))
	}()

	// Convert the namespace/name string into a distinct namespace and name.
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		runtime.HandleError(fmt.Errorf("invalid resource key: %s", key))
		return nil
	}
	if len(namespace) == 0 || len(name) == 0 {
		return fmt.Errorf("invalid job key %q: either namespace or name is missing", key)
	}

	// Get the Pod with this namespace/name.
	pod, err := c.podLister.Pods(namespace).Get(name)
	if err != nil {
		// The MPIJob may no longer exist, in which case we stop processing.
		if errors.IsNotFound(err) {
			klog.V(4).Infof("Pod has been deleted: %v", key)
			return nil
		}
		return err
	}

	if (c.watcherMode == "ready" && pod.Status.Phase == corev1.PodRunning) ||
		(c.watcherMode == "finished" && pod.Status.Phase == corev1.PodSucceeded) {
		c.lock.Lock()
		defer c.lock.Unlock()
		delete(c.watchedPods, pod.Name)
	}

	return nil
}

func (c *WatcherLoopController) enqueue(obj interface{}) {
	var key string
	var err error
	if key, err = cache.MetaNamespaceKeyFunc(obj); err != nil {
		runtime.HandleError(err)
		return
	}
	c.queue.AddRateLimited(key)
}
