package client

import (
	"context"
	"log"
	"os"
	"strconv"

	"github.com/aojea/client-go-multidialer/multidialer"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

const (
	EnvLoadBalancerBypass = "MULTI_DIALER_LOAD_BALANCER_BYPASS"
)

var (
	loadBalancerBypass bool
)

// NewForConfig has two modes:
// 1. Failover Mode:
//		It will always try to connect the original address, fall back to backends only if this connection fails.
// 2. LoadBalancer-Bypass Mode:
//		It will find the backends first, try to connect the backends directly.
//		It will fall back to the original address only if all the backends are down.
//
// When MULTI_DIALER_LOAD_BALANCER_BYPASS is set to true, it will choose the LoadBalancer-Bypass mode.
// if not, it will choose the default Failover mode.

func init() {
	var err error
	if raw := os.Getenv(EnvLoadBalancerBypass); raw != "" {
		loadBalancerBypass, err = strconv.ParseBool(raw)
		if err != nil {
			log.Panicf("Error parsing %s: %v", EnvLoadBalancerBypass, err)
		}
	} else {
		loadBalancerBypass = false
	}
}

// NewForConfig creates a resilient client-go that, in case of connection failures,
// tries to connect to all the available apiservers in the cluster.
func NewForConfig(ctx context.Context, config *rest.Config) (*kubernetes.Clientset, error) {
	if loadBalancerBypass {
		return loadBalancerBypassClient(ctx, config)
	}
	return failoverClient(ctx, config)
}

// failoverClient creates a resilient client-go that, in case of connection failures,
// tries to connect to all the available apiservers in the cluster.
func failoverClient(ctx context.Context, config *rest.Config) (*kubernetes.Clientset, error) {
	// create the clientset
	configShallowCopy := *config
	// it wraps the custom dialer if exists
	d := multidialer.NewDialer(configShallowCopy.Dial)
	// use the multidialier for our clientset
	configShallowCopy.Dial = d.DialContext
	// create the clientset with our own dialer
	cs, err := kubernetes.NewForConfig(&configShallowCopy)
	if err != nil {
		return cs, err
	}
	// start the resolver to update the list of available apiservers
	// !!! using our own dialer !!!
	d.Start(ctx, cs)
	return cs, nil
}

// loadBalancerBypassClient creates a client-go that, always connects to the apiserver backends directly first,
// in case of connection failures, it will fall back to the original load balancer address.
func loadBalancerBypassClient(ctx context.Context, config *rest.Config) (*kubernetes.Clientset, error) {
	var retCs, ownCs *kubernetes.Clientset
	var err error
	configShallowCopy := *config
	// create the clientset for our own dialer
	ownCs, err = kubernetes.NewForConfig(&configShallowCopy)
	if err != nil {
		return ownCs, err
	}
	// it wraps the custom dialer if exists
	d := multidialer.NewDialer(configShallowCopy.Dial)
	// start the resolver to update the list of available apiservers
	d.Start(ctx, ownCs)

	// use the multidialier for to-be-returned clientset
	configShallowCopy.Dial = d.DialContext
	retCs, err = kubernetes.NewForConfig(&configShallowCopy)
	if err != nil {
		return retCs, err
	}
	return retCs, nil
}
