/*
Copyright 2021 The Kubernetes Authors.

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

package loadbalancers

import (
	"context"
	"errors"
	"strings"

	"github.com/GoogleCloudPlatform/k8s-cloud-provider/pkg/cloud/meta"
	"google.golang.org/api/compute/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/cluster-api-provider-gcp/cloud/gcperrors"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// Reconcile reconcile cluster control-plane loadbalancer components.
func (s *Service) Reconcile(ctx context.Context) error {
	log := log.FromContext(ctx)
	log.Info("Reconciling loadbalancer resources")

	// Creates instance groups used by load balancer(s)
	instancegroups, err := s.createOrGetInstanceGroups(ctx)
	if err != nil {
		return err
	}

	// Create LoadBalancer HealthCheck
	healthcheck, err := s.createOrGetHealthCheck(ctx)
	if err != nil {
		return err
	}

	// Create LoadBalancer BackendService
	backendsvc, err := s.createOrGetBackendService(ctx, instancegroups, healthcheck)
	if err != nil {
		return err
	}

	// Create TargetTCPProxy for Proxy Load Balancer
	if !s.scope.IsLoadBalancerInternal() {
		target, err := s.createOrGetTargetTCPProxy(ctx, backendsvc)
		if err != nil {
			return err
		}
	}

	// Create an address for the LoadBalancer
	addr, err := s.createOrGetAddress(ctx)
	if err != nil {
		return err
	}

	// Create a forwarding rule to the backend service
	return s.createOrGetForwardingRule(ctx, target, addr)
}

// Delete deletes cluster control-plane loadbalancer components.
func (s *Service) Delete(ctx context.Context) error {
	log := log.FromContext(ctx)
	log.Info("Deleting loadbalancer resources")

	if err := s.deleteForwardingRule(ctx); err != nil {
		return err
	}

	if err := s.deleteAddress(ctx); err != nil {
		return err
	}

	if err := s.deleteTargetTCPProxy(ctx); err != nil {
		return err
	}

	if err := s.deleteBackendService(ctx); err != nil {
		return err
	}

	if err := s.deleteHealthCheck(ctx); err != nil {
		return err
	}

	return s.deleteInstanceGroups(ctx)
}

func (s *Service) createOrGetInstanceGroups(ctx context.Context) ([]*compute.InstanceGroup, error) {
	log := log.FromContext(ctx)
	fd := s.scope.FailureDomains()
	zones := make([]string, 0, len(fd))
	for zone := range fd {
		zones = append(zones, zone)
	}

	groups := make([]*compute.InstanceGroup, 0, len(zones))
	groupsMap := s.scope.Network().APIServerInstanceGroups
	if groupsMap == nil {
		groupsMap = make(map[string]string)
	}

	for _, zone := range zones {
		instancegroupSpec := s.scope.InstanceGroupSpec(zone)
		log.V(2).Info("Looking for instancegroup in zone", "zone", zone, "name", instancegroupSpec.Name)
		instancegroup, err := s.instancegroups.Get(ctx, meta.ZonalKey(instancegroupSpec.Name, zone))
		if err != nil {
			if !gcperrors.IsNotFound(err) {
				log.Error(err, "Error looking for instancegroup in zone", "zone", zone)
				return groups, err
			}

			log.V(2).Info("Creating instancegroup in zone", "zone", zone, "name", instancegroupSpec.Name)
			if err := s.instancegroups.Insert(ctx, meta.ZonalKey(instancegroupSpec.Name, zone), instancegroupSpec); err != nil {
				log.Error(err, "Error creating instancegroup", "name", instancegroupSpec.Name)
				return groups, err
			}

			instancegroup, err = s.instancegroups.Get(ctx, meta.ZonalKey(instancegroupSpec.Name, zone))
			if err != nil {
				return groups, err
			}
		}

		groups = append(groups, instancegroup)
		groupsMap[zone] = instancegroup.SelfLink
	}

	s.scope.Network().APIServerInstanceGroups = groupsMap
	return groups, nil
}

func (s *Service) createOrGetHealthCheck(ctx context.Context) (*compute.HealthCheck, error) {
	log := log.FromContext(ctx)
	healthcheckSpec := s.scope.HealthCheckSpec()

	var key *meta.Key
	var healthcheck *compute.HealthCheck
	var err error

	log.V(2).Info("Looking for healthcheck", "name", healthcheckSpec.Name)
	if s.scope.IsLoadBalancerInternal() {
		key = meta.RegionalKey(healthcheckSpec.Name, s.scope.Region())
		healthcheck, err = s.regionalhealthchecks.Get(ctx, key)
	} else {
		key = meta.GlobalKey(healthcheckSpec.Name)
		healthcheck, err = s.healthchecks.Get(ctx, key)
	}
	if err != nil {
		if !gcperrors.IsNotFound(err) {
			log.Error(err, "Error looking for healthcheck", "name", healthcheckSpec.Name)
			return nil, err
		}

		log.V(2).Info("Creating a healthcheck", "name", healthcheckSpec.Name)
		// Create a regional or global health check based on the load balancer type
		if s.scope.IsLoadBalancerInternal() {
			if err := s.regionalhealthchecks.Insert(ctx, key, healthcheckSpec); err != nil {
				log.Error(err, "Error creating a regional healthcheck", "name", healthcheckSpec.Name)
				return nil, err
			}
		} else {
			if err := s.healthchecks.Insert(ctx, key, healthcheckSpec); err != nil {
				log.Error(err, "Error creating a global healthcheck", "name", healthcheckSpec.Name)
				return nil, err
			}
		}

		// Get the health check after creation to ensure it was created successfully.
		if s.scope.IsLoadBalancerInternal() {
			healthcheck, err = s.regionalhealthchecks.Get(ctx, key)
		} else {
			healthcheck, err = s.healthchecks.Get(ctx, key)
		}
		if err != nil {
			return nil, err
		}
	}

	return healthcheck, nil
}

func (s *Service) createOrGetBackendService(ctx context.Context, instancegroups []*compute.InstanceGroup, healthcheck *compute.HealthCheck) (*compute.BackendService, error) {
	log := log.FromContext(ctx)

	var balancingMode string
	if s.scope.IsLoadBalancerInternal() {
		balancingMode = "CONNECTION"
	} else {
		balancingMode = "UTILIZATION"
	}

	backends := make([]*compute.Backend, 0, len(instancegroups))
	for _, group := range instancegroups {
		backends = append(backends, &compute.Backend{
			BalancingMode: balancingMode,
			Group:         group.SelfLink,
		})
	}

	backendsvcSpec := s.scope.BackendServiceSpec()
	backendsvcSpec.Backends = backends
	backendsvcSpec.HealthChecks = []string{healthcheck.SelfLink}

	var key *meta.Key
	var backendsvc *compute.BackendService
	var err error

	if s.scope.IsLoadBalancerInternal() {
		key = meta.RegionalKey(backendsvcSpec.Name, s.scope.Region())
		backendsvc, err = s.regionalbackendservices.Get(ctx, key)
	} else {
		key = meta.GlobalKey(backendsvcSpec.Name)
		backendsvc, err = s.backendservices.Get(ctx, key)
	}
	if err != nil {
		if !gcperrors.IsNotFound(err) {
			log.Error(err, "Error looking for backendservice", "name", backendsvcSpec.Name)
			return nil, err
		}

		log.V(2).Info("Creating a backendservice", "name", backendsvcSpec.Name)
		// Create a regional or global backend service based on the load balancer type
		if s.scope.IsLoadBalancerInternal() {
			if err := s.regionalbackendservices.Insert(ctx, key, backendsvcSpec); err != nil {
				log.Error(err, "Error creating a regional backendservice", "name", backendsvcSpec.Name)
				return nil, err
			}
		} else {
			if err := s.backendservices.Insert(ctx, key, backendsvcSpec); err != nil {
				log.Error(err, "Error creating a global backendservice", "name", backendsvcSpec.Name)
				return nil, err
			}
		}

		// Get the backend service after creation to ensure it was created successfully.
		if s.scope.IsLoadBalancerInternal() {
			backendsvc, err = s.regionalbackendservices.Get(ctx, key)
		} else {
			backendsvc, err = s.backendservices.Get(ctx, key)
		}
		if err != nil {
			return nil, err
		}
	}

	if len(backendsvc.Backends) != len(backendsvcSpec.Backends) {
		log.V(2).Info("Updating a backendservice", "name", backendsvcSpec.Name)
		if s.scope.IsLoadBalancerInternal() {
			if err := s.regionalbackendservices.Update(ctx, key, backendsvcSpec); err != nil {
				log.Error(err, "Error updating a regional backendservice", "name", backendsvcSpec.Name)
				return nil, err
			}
		} else {
			if err := s.backendservices.Update(ctx, key, backendsvcSpec); err != nil {
				log.Error(err, "Error updating a global backendservice", "name", backendsvcSpec.Name)
				return nil, err
			}
		}
	}

	s.scope.Network().APIServerBackendService = ptr.To[string](backendsvc.SelfLink)
	return backendsvc, nil
}

func (s *Service) createOrGetTargetTCPProxy(ctx context.Context, service *compute.BackendService) (*compute.TargetTcpProxy, error) {
	log := log.FromContext(ctx)
	targetSpec := s.scope.TargetTCPProxySpec()
	targetSpec.Service = service.SelfLink
	key := meta.GlobalKey(targetSpec.Name)
	target, err := s.targettcpproxies.Get(ctx, key)
	if err != nil {
		if !gcperrors.IsNotFound(err) {
			log.Error(err, "Error looking for targettcpproxy", "name", targetSpec.Name)
			return nil, err
		}

		log.V(2).Info("Creating a targettcpproxy", "name", targetSpec.Name)
		if err := s.targettcpproxies.Insert(ctx, key, targetSpec); err != nil {
			log.Error(err, "Error creating a targettcpproxy", "name", targetSpec.Name)
			return nil, err
		}

		target, err = s.targettcpproxies.Get(ctx, key)
		if err != nil {
			return nil, err
		}
	}

	return target, nil
}

func (s *Service) createOrGetAddress(ctx context.Context) (*compute.Address, error) {
	log := log.FromContext(ctx)
	addrSpec := s.scope.AddressSpec()
	log.V(2).Info("Looking for address", "name", addrSpec.Name)

	var key *meta.Key
	var addr *compute.Address
	var err error
	// Use regional key for internal load balancers
	if s.scope.IsLoadBalancerInternal() {
		key = meta.RegionalKey(addrSpec.Name, s.scope.Region())
		subnet, err := s.getSubnet(ctx)
		if err != nil {
			log.Error(err, "Error getting subnet for Internal Load Balancer")
			return nil, err
		}
		addrSpec.Subnetwork = subnet.SelfLink
		_, err = s.internaladdresses.Get(ctx, key)
	} else {
		key = meta.GlobalKey(addrSpec.Name)
		_, err = s.addresses.Get(ctx, key)
	}
	if err != nil {
		if !gcperrors.IsNotFound(err) {
			log.Error(err, "Error looking for address", "name", addrSpec.Name)
			return nil, err
		}

		log.V(2).Info("Creating a address", "name", addrSpec.Name)
		// Create a regional or global address based on the load balancer type
		if s.scope.IsLoadBalancerInternal() {
			if err := s.internaladdresses.Insert(ctx, key, addrSpec); err != nil {
				log.Error(err, "Error creating a internal address", "name", addrSpec.Name)
				return nil, err
			}
		} else {
			if err := s.addresses.Insert(ctx, key, addrSpec); err != nil {
				log.Error(err, "Error creating a global address", "name", addrSpec.Name)
				return nil, err
			}
		}

		// Get the address after creation to ensure it was created successfully.
		if s.scope.IsLoadBalancerInternal() {
			addr, err = s.internaladdresses.Get(ctx, key)
		} else {
			addr, err = s.addresses.Get(ctx, key)
		}
		if err != nil {
			return nil, err
		}
	}

	return addr, nil
}

func (s *Service) createOrGetForwardingRule(ctx context.Context, target *compute.TargetTcpProxy, addr *compute.Address) error {
	log := log.FromContext(ctx)
	spec := s.scope.ForwardingRuleSpec()
	spec.Target = target.SelfLink
	spec.IPAddress = addr.SelfLink

	var key *meta.Key
	var err error
	log.V(2).Info("Looking for forwardingrule", "name", spec.Name)
	// Use regional key for internal load balancers
	if s.scope.IsLoadBalancerInternal() {
		key = meta.RegionalKey(spec.Name, s.scope.Region())
		_, err = s.regionalforwardingrules.Get(ctx, key)
	} else {
		key = meta.GlobalKey(spec.Name)
		_, err = s.forwardingrules.Get(ctx, key)
	}
	if err != nil {
		if !gcperrors.IsNotFound(err) {
			log.Error(err, "Error looking for forwardingrule", "name", spec.Name)
			return err
		}

		log.V(2).Info("Creating a forwardingrule", "name", spec.Name)
		// Create a regional or global forwarding rule based on the load balancer type
		if s.scope.IsLoadBalancerInternal() {
			if err := s.regionalforwardingrules.Insert(ctx, key, spec); err != nil {
				log.Error(err, "Error creating a regional forwardingrule", "name", spec.Name)
				return err
			}
		} else {
			if err := s.forwardingrules.Insert(ctx, key, spec); err != nil {
				log.Error(err, "Error creating a global forwardingrule", "name", spec.Name)
				return err
			}
		}

		// Get the forwarding rule after creation to ensure it was created successfully.
		if s.scope.IsLoadBalancerInternal() {
			_, err = s.regionalforwardingrules.Get(ctx, key)
		} else {
			_, err = s.forwardingrules.Get(ctx, key)
		}
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *Service) deleteForwardingRule(ctx context.Context) error {
	log := log.FromContext(ctx)
	spec := s.scope.ForwardingRuleSpec()

	log.V(2).Info("Deleting a forwardingrule", "name", spec.Name)

	if s.scope.IsLoadBalancerInternal() {
		key := meta.RegionalKey(spec.Name, s.scope.Region())
		if err := s.regionalforwardingrules.Delete(ctx, key); err != nil && !gcperrors.IsNotFound(err) {
			log.Error(err, "Error updating a forwardingrule", "name", spec.Name)
			return err
		}
	} else {
		key := meta.GlobalKey(spec.Name)
		if err := s.forwardingrules.Delete(ctx, key); err != nil && !gcperrors.IsNotFound(err) {
			log.Error(err, "Error updating a forwardingrule", "name", spec.Name)
			return err
		}
	}

	return nil
}

func (s *Service) deleteAddress(ctx context.Context) error {
	log := log.FromContext(ctx)
	spec := s.scope.AddressSpec()

	log.V(2).Info("Deleting a address", "name", spec.Name)

	if s.scope.IsLoadBalancerInternal() {
		key := meta.RegionalKey(spec.Name, s.scope.Region())
		if err := s.internaladdresses.Delete(ctx, key); err != nil && !gcperrors.IsNotFound(err) {
			log.Error(err, "Error deleting a internal address", "name", spec.Name)
			return err
		}
	} else {
		key := meta.GlobalKey(spec.Name)
		if err := s.addresses.Delete(ctx, key); err != nil && !gcperrors.IsNotFound(err) {
			log.Error(err, "Error deleting a global address", "name", spec.Name)
			return err
		}
	}

	return nil
}

func (s *Service) deleteTargetTCPProxy(ctx context.Context) error {
	log := log.FromContext(ctx)
	spec := s.scope.TargetTCPProxySpec()

	var key *meta.Key
	if s.scope.IsLoadBalancerInternal() {
		key = meta.RegionalKey(spec.Name, s.scope.Region())
	} else {
		key = meta.GlobalKey(spec.Name)
	}

	log.V(2).Info("Deleting a targettcpproxy", "name", spec.Name)
	if err := s.targettcpproxies.Delete(ctx, key); err != nil && !gcperrors.IsNotFound(err) {
		log.Error(err, "Error deleting a targettcpproxy", "name", spec.Name)
		return err
	}

	return nil
}

func (s *Service) deleteBackendService(ctx context.Context) error {
	log := log.FromContext(ctx)
	spec := s.scope.BackendServiceSpec()

	log.V(2).Info("Deleting a backendservice", "name", spec.Name)
	if s.scope.IsLoadBalancerInternal() {
		key := meta.RegionalKey(spec.Name, s.scope.Region())
		if err := s.regionalbackendservices.Delete(ctx, key); err != nil && !gcperrors.IsNotFound(err) {
			log.Error(err, "Error deleting a regional backendservice", "name", spec.Name)
			return err
		}
	} else {
		key := meta.GlobalKey(spec.Name)
		if err := s.backendservices.Delete(ctx, key); err != nil && !gcperrors.IsNotFound(err) {
			log.Error(err, "Error deleting a global backendservice", "name", spec.Name)
			return err
		}
	}
	return nil
}

func (s *Service) deleteHealthCheck(ctx context.Context) error {
	log := log.FromContext(ctx)
	spec := s.scope.HealthCheckSpec()

	log.V(2).Info("Deleting a healthcheck", "name", spec.Name)
	if s.scope.IsLoadBalancerInternal() {
		key := meta.RegionalKey(spec.Name, s.scope.Region())
		if err := s.regionalhealthchecks.Delete(ctx, key); err != nil && !gcperrors.IsNotFound(err) {
			log.Error(err, "Error deleting a regional healthcheck", "name", spec.Name)
			return err
		}
	} else {
		key := meta.GlobalKey(spec.Name)
		if err := s.healthchecks.Delete(ctx, key); err != nil && !gcperrors.IsNotFound(err) {
			log.Error(err, "Error deleting a global healthcheck", "name", spec.Name)
			return err
		}
	}
	return nil
}

func (s *Service) deleteInstanceGroups(ctx context.Context) error {
	log := log.FromContext(ctx)
	for zone := range s.scope.Network().APIServerInstanceGroups {
		spec := s.scope.InstanceGroupSpec(zone)
		key := meta.ZonalKey(spec.Name, zone)
		log.V(2).Info("Deleting a instancegroup", "name", spec.Name)
		if err := s.instancegroups.Delete(ctx, key); err != nil {
			if !gcperrors.IsNotFound(err) {
				log.Error(err, "Error deleting a instancegroup", "name", spec.Name)
				return err
			}

			delete(s.scope.Network().APIServerInstanceGroups, zone)
		}
	}

	return nil
}

// getSubnet gets the subnet to use for an internal Load Balancer.
func (s *Service) getSubnet(ctx context.Context) (*compute.Subnetwork, error) {
	log := log.FromContext(ctx)
	cfgSubnet := ""
	lbSpec := s.scope.LoadBalancer()
	if lbSpec.InternalLoadBalancer != nil {
		cfgSubnet = ptr.Deref(lbSpec.InternalLoadBalancer.Subnet, "")
	}
	for _, subnetSpec := range s.scope.SubnetSpecs() {
		log.V(2).Info("Looking for subnet for load balancer", "name", subnetSpec.Name)
		region := subnetSpec.Region
		if region == "" {
			region = s.scope.Region()
		}

		subnetKey := meta.RegionalKey(subnetSpec.Name, region)
		subnet, err := s.subnets.Get(ctx, subnetKey)
		if err != nil {
			return nil, err
		}
		// Return subnet that matches configuration, or first one if not configured
		if cfgSubnet == "" || strings.HasSuffix(subnet.Name, cfgSubnet) {
			return subnet, nil
		}
	}

	return nil, errors.New("could not find subnet")
}
