package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"reflect"
	"strings"
	"time"

	"github.com/go-logr/logr"
	"github.com/go-logr/zapr"
	"github.com/sourcegraph/conc/pool"
	"github.com/spf13/pflag"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes/scheme"
	clientgocache "k8s.io/client-go/tools/cache"
	watchtools "k8s.io/client-go/tools/watch"
	"k8s.io/klog/v2"
	"k8s.io/kubectl/pkg/polymorphichelpers"
	"k8s.io/kubectl/pkg/util/podutils"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
	"sigs.k8s.io/controller-runtime/pkg/client/config"
)

func getLogger(development bool) *zap.Logger {
	if development {
		cfg := zap.NewDevelopmentConfig()
		cfg.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
		return zap.Must(cfg.Build())
	}
	cfg := zap.NewProductionConfig()
	cfg.DisableCaller = true
	return zap.Must(cfg.Build())
}

var out io.Writer = os.Stderr

func main() {
	if err := run(); err != nil {
		fmt.Fprint(out, err.Error())
		os.Exit(1)
	}
}

const metadataNameKey = "metadata.name"

func run() error {
	var verbose bool
	var quiet bool
	var timeout time.Duration
	var help bool
	pflag.BoolVarP(&verbose, "verbose", "v", false, "Enables more verbose logging")
	pflag.BoolVarP(&quiet, "quiet", "q", false, "Disables all logging, only communication mechanism is exit code")
	pflag.DurationVarP(&timeout, "timeout", "t", 5*time.Minute, "timeout for all operations")
	pflag.BoolVarP(&help, "help", "h", false, "Prints CLI usage")
	pflag.Parse()

	if help {
		pflag.Usage()
		os.Exit(0)
	}

	// silence klog
	klog.SetOutput(io.Discard)
	klog.LogToStderr(false)

	log := getLogger(verbose)
	ctrl.SetLogger(zapr.NewLogger(log))
	if quiet {
		log = zap.NewNop()
		ctrl.SetLogger(logr.Discard())
		out = io.Discard
	}

	restCfg := config.GetConfigOrDie()
	restCfg.Burst = 300
	restCfg.QPS = 200

	ctx, cancel := context.WithTimeout(ctrl.SetupSignalHandler(), timeout)
	defer cancel()

	c, err := cache.New(config.GetConfigOrDie(), cache.Options{})
	if err != nil {
		return err
	}
	for _, obj := range []client.Object{
		&corev1.Node{},
		&corev1.Pod{},
		&appsv1.Deployment{},
		&appsv1.DaemonSet{},
		&appsv1.StatefulSet{},
		&batchv1.Job{},
	} {
		gvk, err := apiutil.GVKForObject(obj, scheme.Scheme)
		if err != nil {
			return err
		}
		unstr := &unstructured.Unstructured{}
		unstr.SetGroupVersionKind(gvk)
		if err := c.IndexField(ctx, unstr, metadataNameKey, func(object client.Object) []string {
			return []string{object.GetName()}
		}); err != nil {
			return err
		}
	}

	go func() {
		if err := c.Start(ctx); err != nil {
			log.Sugar().Errorf("failed to run cache: %s", err)
		}
	}()

	log.Debug("waiting for cache to sync")
	if !c.WaitForCacheSync(ctx) {
		return fmt.Errorf("failed to wait for cache to sync")
	}
	cli, err := client.NewWithWatch(restCfg, client.Options{
		Cache: &client.CacheOptions{
			Reader:       c,
			Unstructured: true,
		},
	})
	if err != nil {
		return err
	}

	p := pool.New().WithErrors().WithContext(ctx).WithFirstError().WithCancelOnError()
	waiter := &waiter{cli: cli, log: log.Sugar()}
	deployList := &appsv1.DeploymentList{}
	if err := cli.List(ctx, deployList); err != nil {
		return err
	}

	for _, deploy := range deployList.Items {
		deploy := deploy
		p.Go(func(ctx context.Context) error {
			return waiter.WaitForWorkload(ctx, deploy.GetName(), deploy.GetNamespace(), appsv1.SchemeGroupVersion.WithKind(reflect.TypeOf(appsv1.DeploymentList{}).Name()), reflect.TypeOf(appsv1.Deployment{}).Name())
		})
	}

	daemonSetList := &appsv1.DaemonSetList{}
	if err := cli.List(ctx, daemonSetList); err != nil {
		return err
	}

	for _, ds := range daemonSetList.Items {
		ds := ds
		p.Go(func(ctx context.Context) error {
			return waiter.WaitForWorkload(ctx, ds.GetName(), ds.GetNamespace(), appsv1.SchemeGroupVersion.WithKind(reflect.TypeOf(appsv1.DaemonSetList{}).Name()), reflect.TypeOf(appsv1.DaemonSet{}).Name())
		})
	}

	stsList := &appsv1.StatefulSetList{}
	if err := cli.List(ctx, stsList); err != nil {
		return err
	}
	for _, sts := range stsList.Items {
		sts := sts
		p.Go(func(ctx context.Context) error {
			return waiter.WaitForWorkload(ctx, sts.GetName(), sts.GetNamespace(), appsv1.SchemeGroupVersion.WithKind(reflect.TypeOf(appsv1.StatefulSetList{}).Name()), reflect.TypeOf(appsv1.StatefulSet{}).Name())
		})
	}

	podList := &corev1.PodList{}
	if err := cli.List(ctx, podList); err != nil {
		return err
	}
	for _, pod := range podList.Items {
		pod := pod
		p.Go(func(ctx context.Context) error {
			return waiter.WaitForPod(ctx, pod.GetName(), pod.GetNamespace())
		})
	}

	nodeList := &corev1.NodeList{}
	if err := cli.List(ctx, nodeList); err != nil {
		return err
	}
	for _, node := range nodeList.Items {
		node := node
		p.Go(func(ctx context.Context) error {
			return waiter.WaitForNode(ctx, node.GetName(), node.GetNamespace())
		})
	}

	jobList := &batchv1.JobList{}
	if err := cli.List(ctx, jobList); err != nil {
		return err
	}
	for _, job := range jobList.Items {
		job := job
		p.Go(func(ctx context.Context) error {
			return waiter.WaitForJob(ctx, job.GetName(), job.GetNamespace())
		})
	}

	if err := p.Wait(); err != nil {
		return err
	}
	log.Info("Finished successfully!")
	return nil
}

type waiter struct {
	cli client.WithWatch
	log *zap.SugaredLogger
}

func (w *waiter) listWatcherFrom(ctx context.Context, name, namespace string, listGvk schema.GroupVersionKind) *clientgocache.ListWatch {
	fieldSelector := fields.OneTermEqualSelector(metadataNameKey, name)
	return &clientgocache.ListWatch{
		ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
			list := &unstructured.UnstructuredList{}
			list.SetGroupVersionKind(listGvk)
			if err := w.cli.List(ctx, list, &client.ListOptions{
				FieldSelector: fieldSelector,
				Namespace:     namespace,
			}); err != nil {
				return nil, err
			}
			return list, nil
		},
		WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
			list := &unstructured.UnstructuredList{}
			list.SetGroupVersionKind(listGvk)
			wi, err := w.cli.Watch(ctx, list, &client.ListOptions{
				FieldSelector: fieldSelector,
				Namespace:     namespace,
			})
			if err != nil {
				return nil, err
			}
			return wi, nil
		},
	}
}

func conditionFn(addedModifiedFn watchtools.ConditionFunc) watchtools.ConditionFunc {
	return func(e watch.Event) (bool, error) {
		switch t := e.Type; t {
		case watch.Added, watch.Modified:
			return addedModifiedFn(e)
		case watch.Deleted:
			// We need to abort to avoid cases of recreation and not to silently watch the wrong (new) object
			return true, fmt.Errorf("object has been deleted")

		default:
			return true, fmt.Errorf("internal error: unexpected event %#v", e)
		}
	}
}

func (w *waiter) WaitForWorkload(ctx context.Context, name, namespace string, listGvk schema.GroupVersionKind, objKind string) error {
	lw := w.listWatcherFrom(ctx, name, namespace, listGvk)

	statusViewer, err := polymorphichelpers.StatusViewerFor(schema.GroupKind{
		Group: listGvk.Group,
		Kind:  objKind,
	})
	if err != nil {
		return err
	}

	log := w.log.With("name", name, "namespace", namespace, "gvk", listGvk.GroupVersion().WithKind(objKind))

	_, err = watchtools.UntilWithSync(ctx, lw, &unstructured.Unstructured{}, nil, conditionFn(func(event watch.Event) (bool, error) {
		status, done, err := statusViewer.Status(event.Object.(runtime.Unstructured), 0)
		if err != nil {
			return false, err
		}
		log.Debug(strings.TrimSpace(status))
		return done, nil
	}))
	return err
}

func (w *waiter) WaitForJob(ctx context.Context, name, namespace string) error {
	log := w.log.With("name", name, "namespace", namespace, "gvk", batchv1.SchemeGroupVersion.WithKind("Job"))
	_, err := watchtools.UntilWithSync(ctx, w.listWatcherFrom(ctx, name, namespace, batchv1.SchemeGroupVersion.WithKind("JobList")), &unstructured.Unstructured{}, nil, conditionFn(func(event watch.Event) (bool, error) {
		job := &batchv1.Job{}
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(event.Object.(*unstructured.Unstructured).Object, job); err != nil {
			return false, err
		}
		for _, cond := range job.Status.Conditions {
			if cond.Type == batchv1.JobComplete && cond.Status == corev1.ConditionTrue {
				log.Debug("JobReady")
				return true, nil
			}
		}
		for _, cond := range job.Status.Conditions {
			if cond.Type == batchv1.JobFailed && cond.Status == corev1.ConditionTrue {
				log.Debug("JobFailed")
				return true, fmt.Errorf("Job %s/%s failed", name, namespace)
			}
		}

		log.Debug("JobNotReady")
		return false, nil
	}))
	return err
}

func (w *waiter) WaitForNode(ctx context.Context, name, namespace string) error {
	log := w.log.With("name", name, "namespace", namespace, "gvk", corev1.SchemeGroupVersion.WithKind("Node"))
	_, err := watchtools.UntilWithSync(ctx, w.listWatcherFrom(ctx, name, namespace, corev1.SchemeGroupVersion.WithKind("NodeList")), &unstructured.Unstructured{}, nil, conditionFn(func(event watch.Event) (bool, error) {
		node := &corev1.Node{}
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(event.Object.(*unstructured.Unstructured).Object, node); err != nil {
			return false, err
		}
		for _, cond := range node.Status.Conditions {
			if cond.Type == corev1.NodeReady && cond.Status == corev1.ConditionTrue {
				log.Debug("NodeReady")
				return true, nil
			}
		}
		log.Debug("NodeNotReady")
		return false, nil
	}))
	return err
}

func (w *waiter) WaitForPod(ctx context.Context, name, namespace string) error {
	log := w.log.With("name", name, "namespace", namespace, "gvk", corev1.SchemeGroupVersion.WithKind("Pod"))
	_, err := watchtools.UntilWithSync(ctx, w.listWatcherFrom(ctx, name, namespace, corev1.SchemeGroupVersion.WithKind("PodList")), &unstructured.Unstructured{}, nil, conditionFn(func(event watch.Event) (bool, error) {
		pod := &corev1.Pod{}
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(event.Object.(*unstructured.Unstructured).Object, pod); err != nil {
			return false, err
		}

		switch pod.Status.Phase {
		case corev1.PodSucceeded:
			log.Debug("PodCompleted")
			return true, nil
		case corev1.PodRunning:
			ready := podutils.IsPodReady(pod)
			if ready {
				log.Debug("PodReady")
			}
			return ready, nil
		case corev1.PodFailed:
			return false, fmt.Errorf("pod %s/%s has status.phase Failed", pod.GetName(), pod.GetNamespace())
		default:
			log.Debugf("pod in phase %q", pod.Status.Phase)
			return false, nil
		}
	}))
	return err
}
