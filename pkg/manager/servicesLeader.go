
package manager

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

// The startServicesWatchForLeaderElection function will start a services watcher, the
func (sm *Manager) startServicesWatchForLeaderElection(ctx context.Context) error {

	err := sm.servicesWatcher(ctx, sm.StartServicesLeaderElection)
	if err != nil {
		return err
	}

	for x := range sm.serviceInstances {
		sm.serviceInstances[x].cluster.Stop()
	}

	log.Infof("Shutting down kube-Vip")

	return nil
}

// The startServicesWatchForLeaderElection function will start a services watcher, the
func (sm *Manager) StartServicesLeaderElection(ctx context.Context, service *v1.Service, wg *sync.WaitGroup) error {

	id, err := os.Hostname()
	if err != nil {
		return err
	}

	serviceLease := fmt.Sprintf("kubevip-%s", service.Name)
	log.Infof("[services election] for service [%s], namespace [%s], lock name [%s], host id [%s]", service.Name, service.Namespace, serviceLease, id)
	// we use the Lease lock type since edits to Leases are less common
	// and fewer objects in the cluster watch "all Leases".
	lock := &resourcelock.LeaseLock{
		LeaseMeta: metav1.ObjectMeta{
			Name:        serviceLease,
			Namespace:   service.Namespace,
			Annotations: map[string]string{"network.io/domain": "kube-vip"},
		},
		Client: sm.clientSet.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{
			Identity: id,
		},
	}

	activeService[string(service.UID)] = true
	leaderService[string(service.UID)] = false
	leaderCtx, leaderCancel := context.WithCancel(ctx)
	// start the leader election code loop
	leaderelection.RunOrDie(leaderCtx, leaderelection.LeaderElectionConfig{
		Lock: lock,
		// IMPORTANT: you MUST ensure that any code you have that
		// is protected by the lease must terminate **before**
		// you call cancel. Otherwise, you could have a background
		// loop still running and another process could
		// get elected before your background loop finished, violating
		// the stated goal of the lease.
		ReleaseOnCancel: true,
		LeaseDuration:   time.Duration(sm.config.LeaseDuration) * time.Second,
		RenewDeadline:   time.Duration(sm.config.RenewDeadline) * time.Second,
		RetryPeriod:     time.Duration(sm.config.RetryPeriod) * time.Second,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(leaderCtx context.Context) {
				// Mark this service as active (as we've started leading)
				// we run this in background as it's blocking
				wg.Add(1)
				go func() {
					if err := sm.syncServices(leaderCtx, service, wg); err != nil {
						activeService[string(service.UID)] = false
						leaderCancel()
					}
				}()

			},
			OnStoppedLeading: func() {
				// we can do cleanup here
				log.Infof("[services election] service [%s] leader lost: [%s]", service.Name, id)
				if activeService[string(service.UID)] {
					if err := sm.deleteService(string(service.UID)); err != nil {
						log.Errorln(err)
					}
				}
				// Mark this service is inactive
				activeService[string(service.UID)] = false
			},
			OnNewLeader: func(identity string) {
				// we're notified when new leader elected
				if identity == id {
					log.Infof("self 2 svc [%s] on host [%s] is assuming leadership of the cluster", service.Name, identity)
					// I just got the lock
					leaderService[string(service.UID)] = true
					return
				}
				log.Infof("[services election] svc [%s] new leader elected: %s", service.Name, identity)
				leaderService[string(service.UID)] = false
				go func() {
					for index := 0; index < 5; index++ {
						if leaderService[string(service.UID)] {
							break
						}
						if sm.clearnServiceLbIp(service) {
							break
						}
						time.Sleep(10 * time.Second)
					}
				}()

			},
		},
	})
	log.Infof("[services election] for service [%s] stopping", service.Name)
	return nil
}
