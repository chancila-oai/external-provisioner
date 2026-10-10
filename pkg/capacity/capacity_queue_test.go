/*
Copyright 2026 The Kubernetes Authors.

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

package capacity

import (
	"context"
	"testing"

	"github.com/kubernetes-csi/external-provisioner/v6/pkg/capacity/topology"
	"github.com/stretchr/testify/require"
	storagev1 "k8s.io/api/storage/v1"
	apierrs "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	fakeclientset "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/util/workqueue"
)

// Use the real queue because the simplified rateLimitingQueue does not track
// items that are processing or marked dirty while they are processing.
func newCapacityTestQueue(t *testing.T) workqueue.TypedRateLimitingInterface[QueueKey] {
	t.Helper()
	queue := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[QueueKey]())
	t.Cleanup(queue.ShutDown)
	return queue
}

func TestCapacityQueueRefreshDeduplicates(t *testing.T) {
	segmentA := topology.Segment{{Key: "node", Value: "a"}}
	segmentB := topology.Segment{{Key: "node", Value: "b"}}
	var items []WorkItem
	for _, segment := range []*topology.Segment{&segmentA, &segmentB, nil} {
		for _, storageClassName := range []string{"sc-a", "sc-b"} {
			items = append(items, WorkItem{segment: segment, storageClassName: storageClassName})
		}
	}

	refreshes := map[string]func(*Controller){
		"poll": func(c *Controller) {
			c.pollCapacities()
		},
		"storage class": func(c *Controller) {
			c.refreshSC("sc-a")
			c.refreshSC("sc-b")
		},
		"add work item": func(c *Controller) {
			c.capacitiesLock.Lock()
			defer c.capacitiesLock.Unlock()
			for _, item := range items {
				c.addWorkItem(item.segment, &storagev1.StorageClass{ObjectMeta: metav1.ObjectMeta{Name: item.storageClassName}})
			}
		},
	}
	for name, refresh := range refreshes {
		t.Run(name, func(t *testing.T) {
			queue := newCapacityTestQueue(t)
			c := &Controller{
				queue:      queue,
				capacities: make(map[WorkItem]*storagev1.CSIStorageCapacity),
			}
			for _, item := range items {
				c.capacities[item] = nil
			}
			for range 3 {
				refresh(c)
			}
			require.Equal(t, len(items), queue.Len(), "repeated refreshes must coalesce by topology and storage class")

			var queuedItems []WorkItem
			for queue.Len() > 0 {
				key, shutdown := queue.Get()
				require.False(t, shutdown)
				require.Nil(t, key.capacity)
				queuedItems = append(queuedItems, WorkItem{segment: key.item.segment, storageClassName: key.item.storageClassName})
				queue.Done(key)
			}
			require.ElementsMatch(t, items, queuedItems, "different topologies and storage classes must remain distinct")
		})
	}
}

func TestCapacityQueueRefreshWhileProcessing(t *testing.T) {
	segment := topology.Segment{{Key: "node", Value: "a"}}
	for name, segment := range map[string]*topology.Segment{"with topology": &segment, "without topology": nil} {
		t.Run(name, func(t *testing.T) {
			queue := newCapacityTestQueue(t)
			item := WorkItem{segment: segment, storageClassName: "sc"}
			c := &Controller{
				queue:      queue,
				capacities: map[WorkItem]*storagev1.CSIStorageCapacity{item: nil},
			}
			c.pollCapacities()
			require.Equal(t, 1, queue.Len())
			processing, shutdown := queue.Get()
			require.False(t, shutdown)

			for range 3 {
				c.pollCapacities()
				c.refreshSC(item.storageClassName)
				c.capacitiesLock.Lock()
				c.addWorkItem(item.segment, &storagev1.StorageClass{ObjectMeta: metav1.ObjectMeta{Name: item.storageClassName}})
				c.capacitiesLock.Unlock()
				require.Zero(t, queue.Len(), "a refresh must not make an item available to another worker while it is processing")
			}

			queue.Done(processing)
			require.Equal(t, 1, queue.Len(), "all refreshes during processing must produce exactly one replay")
			replay, shutdown := queue.Get()
			require.False(t, shutdown)
			require.Equal(t, processing, replay)
			queue.Done(replay)
			require.Zero(t, queue.Len())
		})
	}
}

func TestCapacityQueueProcessesWithoutTopology(t *testing.T) {
	ctx := context.Background()
	capacity := &storagev1.CSIStorageCapacity{
		ObjectMeta:       metav1.ObjectMeta{Name: "capacity", Namespace: ownerNamespace},
		StorageClassName: "sc",
		Capacity:         resource.NewQuantity(1, resource.BinarySI),
	}
	client := fakeclientset.NewSimpleClientset(capacity)
	scInformer := informers.NewSharedInformerFactory(client, 0).Storage().V1().StorageClasses()
	require.NoError(t, scInformer.Informer().GetStore().Add(&storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{Name: capacity.StorageClassName},
	}))
	queue := newCapacityTestQueue(t)
	item := WorkItem{storageClassName: capacity.StorageClassName}
	c := &Controller{
		csiController: &mockCapacity{},
		storageClient: client.StorageV1(),
		queue:         queue,
		scInformer:    scInformer,
		timeout:       timeout,
		capacities:    map[WorkItem]*storagev1.CSIStorageCapacity{item: capacity},
	}
	c.pollCapacities()
	require.Equal(t, 1, queue.Len())
	require.True(t, c.processNextWorkItem(ctx))

	updated, err := client.StorageV1().CSIStorageCapacities(ownerNamespace).Get(ctx, capacity.Name, metav1.GetOptions{})
	require.NoError(t, err)
	// mockCapacity reports zero when GetCapacity has no AccessibleTopology.
	// The changed value proves that the worker refreshed this nil-segment item.
	require.Zero(t, updated.Capacity.Value())
	require.Nil(t, updated.NodeTopology)
	require.Zero(t, queue.Len())
}

func TestCapacityQueueDeletesObsoleteCapacity(t *testing.T) {
	ctx := context.Background()
	obsolete := &storagev1.CSIStorageCapacity{
		ObjectMeta: metav1.ObjectMeta{Name: "obsolete", Namespace: ownerNamespace},
	}
	client := fakeclientset.NewSimpleClientset(obsolete)
	queue := newCapacityTestQueue(t)
	c := &Controller{queue: queue, storageClient: client.StorageV1()}
	queue.Add(QueueKey{capacity: obsolete})
	require.Equal(t, 1, queue.Len())
	require.True(t, c.processNextWorkItem(ctx))

	_, err := client.StorageV1().CSIStorageCapacities(ownerNamespace).Get(ctx, obsolete.Name, metav1.GetOptions{})
	require.True(t, apierrs.IsNotFound(err), "obsolete capacity must be deleted, got: %v", err)
	require.Zero(t, queue.Len())
}
