// Copyright 2020 The Kubeflow Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package app

import (
	"bufio"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubeinformers "k8s.io/client-go/informers"
	kubeclientset "k8s.io/client-go/kubernetes"
	restclientset "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog"
	"k8s.io/sample-controller/pkg/signals"

	"github.com/Qihoo360/dgl-operator/watcher-loop/app/options"
	watcher_loop "github.com/Qihoo360/dgl-operator/watcher-loop/controllers"
)

const (
	RecommendedKubeConfigPathEnv = "KUBECONFIG"
	nsEnvironmentName            = "NAMESPACE"
	fileEnvironmentName          = "WATCHERFILE"
	launcherPodSuffix            = "launcher"
	partitionerPodSuffix         = "partitioner"
)

func Run(opt *options.ServerOption) error {
	namespace := os.Getenv(nsEnvironmentName)
	filename := os.Getenv(fileEnvironmentName)
	if len(namespace) == 0 {
		klog.Infof("%s not set, use default namespace", nsEnvironmentName)
		namespace = metav1.NamespaceDefault
	}
	if opt.Namespace != "" {
		namespace = opt.Namespace
	}

	if opt.WatcherFile != "" {
		filename = opt.WatcherFile
	}

	if len(opt.WatcherMode) == 0 {
		klog.Fatal("WatcherMode (env WATCHERMODE) not set")
	}
	mode := opt.WatcherMode

	if namespace == corev1.NamespaceAll {
		klog.Info("Using cluster scoped operator")
	} else {
		klog.Infof("Scoping operator to namespace %s", namespace)
	}

	// To help debugging, immediately log opts.
	klog.Infof("Server options: %+v", opt)

	// set up signals so we handle the first shutdown signal gracefully
	stopCh := signals.SetupSignalHandler()

	// Note: ENV KUBECONFIG will overwrite user defined Kubeconfig option.
	if len(os.Getenv(RecommendedKubeConfigPathEnv)) > 0 {
		// use the current context in kubeconfig
		// This is very useful for running locally.
		opt.Kubeconfig = os.Getenv(RecommendedKubeConfigPathEnv)
	}

	cfg, err := clientcmd.BuildConfigFromFlags(opt.MasterURL, opt.Kubeconfig)
	if err != nil {
		klog.Fatalf("Error building kubeConfig: %v", err)
	}
	kubeClient, err := kubeclientset.NewForConfig(restclientset.AddUserAgent(cfg, "dgl-operator-watcher-loop"))
	if err != nil {
		klog.Fatalf("Error building kubeClient: %v", err)
	}

	kubeInformerFactory := kubeinformers.NewSharedInformerFactoryWithOptions(kubeClient, 0, kubeinformers.WithNamespace(namespace))

	fp, err := os.Open(filepath.Clean(filename))
	if err != nil {
		klog.Fatalf("Error open file[%s]: %v", filename, err)
	}
	defer func() {
		if err := fp.Close(); err != nil {
			klog.Fatalf("Error closing file: %s\n", err)
		}
	}()
	bufReader := bufio.NewReader(fp)
	pods := []string{}

	for {
		line, _, err := bufReader.ReadLine()
		if err != nil {
			if err == io.EOF {
				break
			}
			continue
		}
		lines := strings.SplitN(string(line), " ", 4)
		if !strings.HasSuffix(lines[2], launcherPodSuffix) {
			pods = append(pods, lines[2])
		}
	}
	controller := watcher_loop.NewWatcherLoopController(
		namespace,
		kubeClient,
		kubeInformerFactory.Core().V1().Pods(),
		pods,
		mode)

	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()

	go kubeInformerFactory.Start(ctx.Done())

	if err = controller.Run(opt.Threadiness, stopCh); err != nil {
		klog.Fatalf("Error running controller: %s", err.Error())
	}

	return nil
}
