
package manager

import (
	"context"
	"fmt"
	"os"
	"sync"

	"github.com/davecgh/go-spew/spew"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/cache"
	watchtools "k8s.io/client-go/tools/watch"
)

// TODO: Fix the naming of these contexts

// activeServiceLoadBalancer keeps track of services that already have a leaderElection in place
var activeServiceLoadBalancer map[string]context.Context

// activeServiceLoadBalancer keeps track of services that already have a leaderElection in place
var activeServiceLoadBalancerCancel map[string]func()

// activeService keeps track of services that already have a leaderElection in place
var activeService map[string]bool

// watchedService keeps track of services that are already being watched
var watchedService map[string]bool

// leaderService keeps track of services that are already being leader
var leaderService map[string]bool

func init() {
	// Set up the caches for monitoring existing active or watched services
	activeServiceLoadBalancerCancel = make(map[string]func())
	activeServiceLoadBalancer = make(map[string]context.Context)
	activeService = make(map[string]bool)
	watchedService = make(map[string]bool)
	leaderService = make(map[string]bool)
}

// This function handles the watching of a services endpoints and updates a load balancers endpoint configurations accordingly
func (sm *Manager) servicesWatcher(ctx context.Context, serviceFunc func(context.Context, *v1.Service, *sync.WaitGroup) error) error {
	// Watch function
	var wg sync.WaitGroup

	id, err := os.Hostname()
	if err != nil {
		return err
	}
	if sm.config.ServiceNamespace == "" {
		// v1.NamespaceAll is actually "", but we'll stay with the const incase things change upstream
		sm.config.ServiceNamespace = v1.NamespaceAll
		log.Infof("starting services watcher for all namespaces")
	} else {
		log.Infof("starting services watcher for services in namespace [%s]", sm.config.ServiceNamespace)
	}

	// Use a restartable watcher, as this should help in the event of etcd or timeout issues
	rw, err := watchtools.NewRetryWatcher("1", &cache.ListWatch{
		WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
			return sm.clientSet.CoreV1().Services(sm.config.ServiceNamespace).Watch(ctx, metav1.ListOptions{})
		},
	})
	if err != nil {
		return fmt.Errorf("error creating services watcher: %s", err.Error())
	}
	exitFunction := make(chan struct{})
	go func() {
		select {
		case <-sm.shutdownChan:
			log.Debug("[services] shutdown called")
			// Stop the retry watcher
			rw.Stop()
			return
		case <-exitFunction:
			log.Debug("[services] function ending")
			// Stop the retry watcher
			rw.Stop()
			return
		}
	}()
	ch := rw.ResultChan()

	// Used for tracking an active endpoint / pod
	for event := range ch {
		sm.countServiceWatchEvent.With(prometheus.Labels{"type": string(event.Type)}).Add(1)

		// We need to inspect the event and get ResourceVersion out of it
		switch event.Type {
		case watch.Added, watch.Modified:

			svc, ok := event.Object.(*v1.Service)
			if !ok {
				return fmt.Errorf("unable to parse Kubernetes services from API watcher")
			}

			// We only care about LoadBalancer services
			if svc.Spec.Type != v1.ServiceTypeLoadBalancer {
				if activeService[string(svc.UID)] {
					log.Infof("watch svc name: %s, type:%s, lbip:%s\n", svc.Name, svc.Spec.Type, svc.Spec.LoadBalancerIP)
					// We can ignore this service
					if svc.Annotations["kube-vip.io/ignore"] == "true" {
						log.Infof("service [%s] has an ignore annotation for kube-vip", svc.Name)
						break
					}

					var svc_ret *v1.Service = nil
					var oldLbIp string
					for x := range sm.serviceInstances {
						log.Debugf("watch Looking for [%s], found [%s]", svc.UID, sm.serviceInstances[x].UID)

						if sm.serviceInstances[x].UID == string(svc.UID) {
							oldLbIp = sm.serviceInstances[x].Vip
						}
					}
					if oldLbIp != "" {
						svc_ret = sm.checkDupLbIP(oldLbIp, string(svc.UID))
					}

					if svc_ret != nil {
						log.Infof("service [%s/%s] has been modified Annotations[kube-vip.io/ignore]: false", svc_ret.Namespace, svc_ret.Name)
						sm.updateServiceIgnoreAnnotationAndLabels(false, svc_ret)
					}

					err := sm.deleteService(string(svc.UID))
					if err != nil {
						log.Error(err)
					}
					// Calls the cancel function of the context
					activeServiceLoadBalancerCancel[string(svc.UID)]()
					activeService[string(svc.UID)] = false
					watchedService[string(svc.UID)] = false
				}
				break
			}

			// We only care about LoadBalancer services that have been allocated an address
			if svc.Spec.LoadBalancerIP == "" {
				if activeService[string(svc.UID)] {
					log.Infof("watch svc name: %s, type:%s, lbip:%s\n", svc.Name, svc.Spec.Type, svc.Spec.LoadBalancerIP)
					// We can ignore this service
					if svc.Annotations["kube-vip.io/ignore"] == "true" {
						log.Infof("service [%s] has an ignore annotation for kube-vip", svc.Name)
						break
					}

					var svc_ret *v1.Service = nil
					var oldLbIp string
					for x := range sm.serviceInstances {
						log.Debugf("watch Looking for [%s], found [%s]", svc.UID, sm.serviceInstances[x].UID)

						if sm.serviceInstances[x].UID == string(svc.UID) {
							oldLbIp = sm.serviceInstances[x].Vip
						}
					}
					if oldLbIp != "" {
						svc_ret = sm.checkDupLbIP(oldLbIp, string(svc.UID))
					}

					if svc_ret != nil {
						log.Infof("service [%s/%s] has been modified Annotations[kube-vip.io/ignore]: false", svc_ret.Namespace, svc_ret.Name)
						sm.updateServiceIgnoreAnnotationAndLabels(false, svc_ret)
					}

					err := sm.deleteService(string(svc.UID))
					if err != nil {
						log.Error(err)
					}
					// Calls the cancel function of the context
					activeServiceLoadBalancerCancel[string(svc.UID)]()
					activeService[string(svc.UID)] = false
					watchedService[string(svc.UID)] = false
				}
				break
			}

			// Check the loadBalancer class
			if svc.Spec.LoadBalancerClass != nil {
				// if this isn't nil then it has been configured, check if it the kube-vip loadBalancer class
				if *svc.Spec.LoadBalancerClass != "kube-vip.io/kube-vip-class" {
					log.Infof("service [%s] specified the loadBalancer class [%s], ignoring", svc.Name, *svc.Spec.LoadBalancerClass)
					break
				}
			} else if sm.config.LoadBalancerClassOnly {
				// if kube-vip is configured to only recognize services with kube-vip's lb class, then ignore the services without any lb class
				log.Infof("kube-vip configured to only recognize services with kube-vip's lb class but the service [%s] didn't specify any loadBalancer class, ignoring", svc.Name)
				break
			}

			// Check if we ignore this service
			if svc.Annotations["kube-vip.io/ignore"] == "true" {
				log.Infof("service [%s] has an ignore annotation for kube-vip", svc.Name)
				break
			}

			changeLbIP := false
			newServiceAddress := svc.Spec.LoadBalancerIP
			newServiceUID := string(svc.UID)
			for x := range sm.serviceInstances {
				if sm.serviceInstances[x].UID == newServiceUID {
					// If the found instance's DHCP configuration doesn't match the new service, delete it.
					if len(svc.Status.LoadBalancer.Ingress) > 0 &&
						newServiceAddress != svc.Status.LoadBalancer.Ingress[0].IP {
						changeLbIP = true
						break
					}
				}
			}
			if changeLbIP {
				log.Debugf("delete newServiceAddress: %s", newServiceAddress)
				err := sm.deleteService(string(svc.UID))
				if err != nil {
					log.Error(err)
				}
				log.Debugf("cancel newServiceAddress: %s", newServiceAddress)
				// Calls the cancel function of the context
				activeServiceLoadBalancerCancel[string(svc.UID)]()
				activeService[string(svc.UID)] = false
				watchedService[string(svc.UID)] = false
				break
			}

			log.Debugf("service [%s] has been added/modified with addresses [%s] and is active [%t]", svc.Name, svc.Spec.LoadBalancerIP, activeService[string(svc.UID)])

			// Scenarios:
			// 1.
			if !activeService[string(svc.UID)] {
				wg.Add(1)
				activeServiceLoadBalancer[string(svc.UID)], activeServiceLoadBalancerCancel[string(svc.UID)] = context.WithCancel(context.TODO())
				// Background the services election
				if sm.config.EnableServicesElection {
					if svc.Spec.ExternalTrafficPolicy == v1.ServiceExternalTrafficPolicyTypeLocal {
						// Start an endpoint watcher if we're not watching it already
						if !watchedService[string(svc.UID)] {
							// background the endpoint watcher
							go func() {
								if svc.Spec.ExternalTrafficPolicy == v1.ServiceExternalTrafficPolicyTypeLocal {
									// Add Endpoint watcher
									wg.Add(1)
									err = sm.watchEndpoint(activeServiceLoadBalancer[string(svc.UID)], id, svc, &wg)
									if err != nil {
										log.Error(err)
									}
									wg.Done()
								}
							}()
							// We're now watching this service
							watchedService[string(svc.UID)] = true
						}
					} else {
						// Increment the waitGroup before the service Func is called (Done is completed in there)
						wg.Add(1)
						go func() {
							err = serviceFunc(activeServiceLoadBalancer[string(svc.UID)], svc, &wg)
							if err != nil {
								log.Error(err)
							}
							wg.Done()
						}()
					}
				} else {
					// Increment the waitGroup before the service Func is called (Done is completed in there)
					wg.Add(1)
					err = serviceFunc(activeServiceLoadBalancer[string(svc.UID)], svc, &wg)
					if err != nil {
						activeService[string(svc.UID)] = false
						watchedService[string(svc.UID)] = false
						log.Error(err)
					}
					wg.Done()
				}
				activeService[string(svc.UID)] = true
			}
		case watch.Deleted:
			svc, ok := event.Object.(*v1.Service)
			if !ok {
				return fmt.Errorf("unable to parse Kubernetes services from API watcher")
			}

			if activeService[string(svc.UID)] {
				// We only care about LoadBalancer services
				if svc.Spec.Type != v1.ServiceTypeLoadBalancer {
					break
				}

				// We can ignore this service
				if svc.Annotations["kube-vip.io/ignore"] == "true" {
					log.Infof("service [%s] has an ignore annotation for kube-vip", svc.Name)
					break
				}

				var svc_ret *v1.Service = nil
				if svc.Spec.LoadBalancerIP != "" {
					svc_ret = sm.checkDupLbIP(svc.Spec.LoadBalancerIP, string(svc.UID))
				}

				// If this is an active service then and additional leaderElection will handle stopping
				err := sm.deleteService(string(svc.UID))
				if err != nil {
					log.Error(err)
				}
				// Calls the cancel function of the context
				activeServiceLoadBalancerCancel[string(svc.UID)]()
				activeService[string(svc.UID)] = false
				watchedService[string(svc.UID)] = false

				if svc_ret != nil {
					log.Infof("service [%s/%s] has been modified Annotations[kube-vip.io/ignore]: false", svc_ret.Namespace, svc_ret.Name)
					sm.updateServiceIgnoreAnnotationAndLabels(false, svc_ret)
				}

				log.Infof("service [%s/%s] has been deleted", svc.Namespace, svc.Name)
			}
		case watch.Bookmark:
			// Un-used
		case watch.Error:
			log.Error("Error attempting to watch Kubernetes services")

			// This round trip allows us to handle unstructured status
			errObject := apierrors.FromObject(event.Object)
			statusErr, ok := errObject.(*apierrors.StatusError)
			if !ok {
				log.Errorf(spew.Sprintf("Received an error which is not *metav1.Status but %#+v", event.Object))

			}

			status := statusErr.ErrStatus
			log.Errorf("services -> %v", status)
		default:
		}
	}
	close(exitFunction)
	log.Warnln("Stopping watching services for type: LoadBalancer in all namespaces")
	return nil
}
