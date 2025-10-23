/*
Copyright 2022.

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

package bpfmanoperator

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"github.com/bpfman/bpfman-operator/apis/v1alpha1"
	bpfmaniov1alpha1 "github.com/bpfman/bpfman-operator/apis/v1alpha1"
	internal "github.com/bpfman/bpfman-operator/internal"
	bpfmanHelpers "github.com/bpfman/bpfman-operator/pkg/helpers"
	"github.com/go-logr/logr"
)

// +kubebuilder:rbac:groups=bpfman.io,resources=clusterbpfapplicationstates,verbs=get;list;watch
// +kubebuilder:rbac:groups=bpfman.io,resources=bpfapplicationstates,verbs=get;list;watch
// +kubebuilder:rbac:groups=bpfman.io,namespace=bpfman,resources=bpfapplicationstates,verbs=get;list;watch
// +kubebuilder:rbac:groups=core,resources=nodes,verbs=get;list;watch

const (
	retryDurationOperator = 5 * time.Second
)

type BpfProgOper interface {
	GetName() string
	GetLabels() map[string]string
	GetConditions() []metav1.Condition
}

type BpfProgListOper[T any] interface {
	// bpfmniov1alpha1.BpfApplicationStateList | bpfmaniov1alpha1.ClusterBpfApplicationStateList
	GetItems() []T
}

type ReconcilerCommon[T BpfProgOper, TL BpfProgListOper[T]] struct {
	client.Client
	Scheme *runtime.Scheme
	Logger logr.Logger
}

// ApplicationReconciler defines a k8s reconciler which can program bpfman.
type ApplicationReconciler[T BpfProgOper, TL BpfProgListOper[T]] interface {
	// BPF Cluster of Namespaced Reconciler
	getAppStateList(ctx context.Context,
		appName string,
		appNamespace string,
	) (*TL, error)
	containsFinalizer(bpfApplication *T, finalizer string) bool

	// *Program Reconciler
	getRecCommon() *ReconcilerCommon[T, TL]
	updateStatus(ctx context.Context,
		namespace string,
		name string,
		cond bpfmaniov1alpha1.BpfApplicationConditionType,
		message string) (ctrl.Result, error)
	getFinalizer() string
}

func reconcileBpfApplication[T BpfProgOper, TL BpfProgListOper[T]](
	ctx context.Context,
	rec ApplicationReconciler[T, TL],
	app client.Object,
) (ctrl.Result, error) {
	r := rec.getRecCommon()
	appName := app.GetName()
	appNamespace := app.GetNamespace()

	r.Logger.V(1).Info("Reconciling Bpf Application", "Namespace", appNamespace, "Name", appName)

	if !controllerutil.ContainsFinalizer(app, internal.BpfmanOperatorFinalizer) {
		r.Logger.V(1).Info("Add Finalizer", "Namespace", appNamespace, "ProgramName", appName)
		return r.addFinalizer(ctx, app, internal.BpfmanOperatorFinalizer)
	}

	// reconcile BpfApplication Objects on all other events
	// list all existing BpfApplication state for the given Program
	bpfAppStateObjs, err := rec.getAppStateList(ctx, appName, appNamespace)
	if err != nil {
		r.Logger.Error(err, "failed to get freshPrograms for full reconcile")
		return ctrl.Result{}, nil
	}

	// List all nodes since a BpfApplicationState object will always be created for each
	nodes := &corev1.NodeList{}
	if err := r.List(ctx, nodes, &client.ListOptions{}); err != nil {
		r.Logger.Error(err, "failed getting nodes for full reconcile")
		return ctrl.Result{Requeue: true, RequeueAfter: retryDurationOperator}, nil
	}

	// If the application isn't being deleted, make sure that each node has at
	// least one BpfApplicationState object.  If not, Return Pending Status.
	if app.GetDeletionTimestamp().IsZero() {
		for _, node := range nodes.Items {
			nodeFound := false
			for _, appState := range (*bpfAppStateObjs).GetItems() {
				bpfProgramState := appState.GetLabels()[internal.K8sHostLabel]
				if node.Name == bpfProgramState {
					nodeFound = true
					break
				}
			}
			if !nodeFound {
				return rec.updateStatus(ctx, appNamespace, appName, bpfmaniov1alpha1.BpfAppCondPending, "")
			}
		}
	}

	pendingBpfApplications := []string{}
	failedBpfApplications := []string{}
	finalApplied := []string{}
	// Make sure no BpfApplications had any issues in the loading or unloading process
	for _, bpfAppState := range (*bpfAppStateObjs).GetItems() {

		if rec.containsFinalizer(&bpfAppState, rec.getFinalizer()) {
			finalApplied = append(finalApplied, bpfAppState.GetName())
		}

		conditions := bpfAppState.GetConditions()
		if bpfmanHelpers.IsBpfAppStateConditionFailure(conditions) {
			failedBpfApplications = append(failedBpfApplications, bpfAppState.GetName())
		} else if bpfmanHelpers.IsBpfAppStateConditionPending(conditions) {
			pendingBpfApplications = append(pendingBpfApplications, bpfAppState.GetName())
		}
	}

	if !app.GetDeletionTimestamp().IsZero() {
		// Only remove bpfman-operator finalizer if all BpfApplicationState objects are ready to be pruned  (i.e there are no
		// bpfPrograms with a finalizer)
		if len(finalApplied) == 0 {
			// Causes Requeue
			return r.removeFinalizer(ctx, app, internal.BpfmanOperatorFinalizer)
		}

		return rec.updateStatus(ctx, appNamespace, appName, bpfmaniov1alpha1.BpfAppCondDeleteError,
			fmt.Sprintf("Program Deletion failed on the following BpfApplicationState objects: %v", finalApplied))
	}

	if len(failedBpfApplications) != 0 {
		return rec.updateStatus(ctx, appNamespace, appName, bpfmaniov1alpha1.BpfAppCondError,
			fmt.Sprintf("BpfApplication Reconciliation failed on the following BpfApplicationState objects: %v", failedBpfApplications))
	} else if len(pendingBpfApplications) != 0 {
		return rec.updateStatus(ctx, appNamespace, appName, bpfmaniov1alpha1.BpfAppCondPending,
			fmt.Sprintf("BpfApplication Reconciliation is pending on the following BpfApplicationState objects: %v", pendingBpfApplications))
	}
	return rec.updateStatus(ctx, appNamespace, appName, bpfmaniov1alpha1.BpfAppCondSuccess, "")
}

func (r *ReconcilerCommon[T, TL]) removeFinalizer(ctx context.Context, bpfApp client.Object, finalizer string) (ctrl.Result, error) {
	r.Logger.Info("Calling KubeAPI to delete Program Finalizer", "Type", bpfApp.GetObjectKind().GroupVersionKind().Kind, "Name", bpfApp.GetName())

	if changed := controllerutil.RemoveFinalizer(bpfApp, finalizer); changed {
		err := r.Update(ctx, bpfApp)
		if err != nil {
			r.Logger.Error(err, "failed to remove bpfApp Finalizer")
			return ctrl.Result{Requeue: true, RequeueAfter: retryDurationOperator}, nil
		}
	}

	return ctrl.Result{}, nil
}

func (r *ReconcilerCommon[T, TL]) addFinalizer(ctx context.Context, app client.Object, finalizer string) (ctrl.Result, error) {
	controllerutil.AddFinalizer(app, finalizer)

	r.Logger.Info("Calling KubeAPI to add Program Finalizer", "Type", app.GetObjectKind().GroupVersionKind().Kind, "Name", app.GetName())
	err := r.Update(ctx, app)
	if err != nil {
		r.Logger.V(1).Info("failed adding bpfman-operator finalizer to Program...requeuing")
		return ctrl.Result{Requeue: true, RequeueAfter: retryDurationOperator}, nil
	}

	return ctrl.Result{}, nil
}

func (r *ReconcilerCommon[T, TL]) updateCondition(
	ctx context.Context,
	obj client.Object,
	conditions []metav1.Condition,
	cond bpfmaniov1alpha1.BpfApplicationConditionType,
	message string,
) (ctrl.Result, error) {
	r.Logger.V(1).Info("updateCondition()", "existing conds", conditions, "new cond", cond)

	c := Condition(cond, message)
	if !meta.SetStatusCondition(&conditions, c) {
		return ctrl.Result{}, nil
	}

	r.Logger.Info("Calling KubeAPI to update Program condition", "Type", obj.GetObjectKind().GroupVersionKind().Kind,
		"Name", obj.GetName(), "condition", c.Type)
	if err := r.Status().Update(ctx, obj); err != nil {
		r.Logger.V(1).Info("failed to set BpfApplication object status...requeuing", "error", err)
		return ctrl.Result{}, err
	}

	r.Logger.V(1).Info("condition updated", "new condition", cond)
	return ctrl.Result{}, nil
}

// Condition is a helper method to promote any given BpfApplicationConditionType
// to a full metav1.Condition in an opinionated fashion.
//
// TODO: this was created in the early days to provide at least SOME status
// information to the user, but the hardcoded messages need to be replaced in
// the future with dynamic and situation-aware messages later.
//
// See: https://github.com/bpfman/bpfman/issues/430
func Condition(b v1alpha1.BpfApplicationConditionType, message string) metav1.Condition {
	cond := metav1.Condition{}

	switch b {
	case v1alpha1.BpfAppCondPending:
		if len(message) == 0 {
			message = "Waiting for Bpf Application Object to be reconciled on all nodes"
		}
		condType := string(v1alpha1.BpfAppCondPending)
		cond = metav1.Condition{
			Type:    condType,
			Status:  metav1.ConditionTrue,
			Reason:  "Pending",
			Message: message,
		}
	case v1alpha1.BpfAppCondError:
		if len(message) == 0 {
			message = "An error has occurred on one or more nodes"
		}
		condType := string(v1alpha1.BpfAppCondError)
		cond = metav1.Condition{
			Type:    condType,
			Status:  metav1.ConditionTrue,
			Reason:  "Error",
			Message: message,
		}
	case v1alpha1.BpfAppCondSuccess:
		if len(message) == 0 {
			message = "BPF application configuration successfully applied on all nodes"
		}
		condType := string(v1alpha1.BpfAppCondSuccess)
		cond = metav1.Condition{
			Type:    condType,
			Status:  metav1.ConditionTrue,
			Reason:  "Success",
			Message: message,
		}
	case v1alpha1.BpfAppCondDeleteError:
		if len(message) == 0 {
			message = "Deletion failed on one or more nodes"
		}
		condType := string(v1alpha1.BpfAppCondDeleteError)
		cond = metav1.Condition{
			Type:    condType,
			Status:  metav1.ConditionTrue,
			Reason:  "DeleteError",
			Message: message,
		}
	}

	return cond
}
