package redis

import (
	"context"
	"fmt"
	"net"
	"regexp"
	"strconv"
	"sync"

	rediscli "github.com/go-redis/redis/v8"
	redisfailoverv1 "github.com/spotahome/redis-operator/api/redisfailover/v1"
	"github.com/spotahome/redis-operator/log"
	"github.com/spotahome/redis-operator/metrics"
	"github.com/spotahome/redis-operator/service/k8s"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

const (
	redisMasterPortREString = "master_port:([0-9.]+)"
)

var (
	redisMasterPortRE = regexp.MustCompile(redisMasterPortREString)
)

type ClientWrapper struct {
	*client
}

func (c *ClientWrapper) GetSlaveOf(ip, port, password string) (string, error) {
	options := &rediscli.Options{
		Addr:     net.JoinHostPort(ip, port),
		Password: password,
		DB:       0,
	}
	rClient := rediscli.NewClient(options)
	defer rClient.Close()
	info, err := rClient.Info(context.TODO(), "replication").Result()
	if err != nil {
		c.metricsRecorder.RecordRedisOperation(metrics.KIND_REDIS, ip, metrics.GET_SLAVE_OF, metrics.FAIL, getRedisError(err))
		log.Errorf("error while getting masterIP : Failed to get info replication while querying redis instance %v", ip)
		return "", err
	}

	// 先尝试寻找port,因为 nodeport 是唯一的
	match := redisMasterPortRE.FindStringSubmatch(info)
	if len(match) != 0 {
		if match[1] != "6379" {
			matchIP, found := NodeportToPodIP(match[1])
			if found {
				return matchIP, nil
			}
		}
	}

	match = redisMasterHostRE.FindStringSubmatch(info)
	if len(match) == 0 {
		c.metricsRecorder.RecordRedisOperation(metrics.KIND_REDIS, ip, metrics.GET_SLAVE_OF, metrics.SUCCESS, metrics.NOT_APPLICABLE)
		return "", nil
	}
	c.metricsRecorder.RecordRedisOperation(metrics.KIND_REDIS, ip, metrics.GET_SLAVE_OF, metrics.SUCCESS, metrics.NOT_APPLICABLE)
	return match[1], nil
}

func (c *ClientWrapper) GetSentinelMonitor(ip string) (string, string, error) {
	rip, rport, err := c.client.GetSentinelMonitor(ip)
	if err != nil {
		return "", "", err
	}
	podIP, found := NodeportToPodIP(rport)
	if found {
		return podIP, "6379", nil
	}
	return rip, rport, err
}

func (c *ClientWrapper) IsMaster(ip, port, password string) (bool, error) {
	nip, nport, found := PodIPToNodePortIP(ip)
	if found {
		ip = nip
		port = nport
	}
	return c.client.IsMaster(ip, port, password)
}

func (c *ClientWrapper) MonitorRedisWithPort(ip, monitor, port, quorum, password string) error {
	nip, nport, found := PodIPToNodePortIP(monitor)
	if found {
		monitor = nip
		port = nport
	}
	return c.client.MonitorRedisWithPort(ip, monitor, port, quorum, password)
}

func (c *ClientWrapper) MakeMaster(ip, port, password string) error {
	nip, nport, found := PodIPToNodePortIP(ip)
	if found {
		ip = nip
		port = nport
	}
	return c.client.MakeMaster(ip, port, password)
}

func (c *ClientWrapper) MakeSlaveOfWithPort(ip, masterIP, masterPort, password string) error {
	sip := ip
	sport := masterPort
	nip, nport, found := PodIPToNodePortIP(ip)
	if found {
		sip = nip
		sport = nport
	}

	nip, nport, found = PodIPToNodePortIP(masterIP)
	if found {
		masterIP = nip
		masterPort = nport
	}

	options := &rediscli.Options{
		Addr:     net.JoinHostPort(sip, sport), // this is IP and Port for the RedisFailover redis
		Password: password,
		DB:       0,
	}
	rClient := rediscli.NewClient(options)
	defer rClient.Close()
	if res := rClient.SlaveOf(context.TODO(), masterIP, masterPort); res.Err() != nil {
		c.metricsRecorder.RecordRedisOperation(metrics.KIND_REDIS, ip, metrics.MAKE_SLAVE_OF, metrics.FAIL, getRedisError(res.Err()))
		return res.Err()
	}
	c.metricsRecorder.RecordRedisOperation(metrics.KIND_REDIS, ip, metrics.MAKE_SLAVE_OF, metrics.SUCCESS, metrics.NOT_APPLICABLE)
	return nil
}

func (c *ClientWrapper) SetCustomRedisConfig(ip string, port string, configs []string, password string) error {
	nip, nport, found := PodIPToNodePortIP(ip)
	if found {
		ip = nip
		port = nport
	}
	return c.client.SetCustomRedisConfig(ip, port, configs, password)
}

func (c *ClientWrapper) SlaveIsReady(ip, port, password string) (bool, error) {
	nip, nport, found := PodIPToNodePortIP(ip)
	if found {
		ip = nip
		port = nport
	}
	return c.client.SlaveIsReady(ip, port, password)
}

var _ Client = &ClientWrapper{}

func NodeportToPodIP(nodeport string) (podIP string, found bool) {
	globalRW.RLock()
	defer globalRW.RUnlock()
	cache, ok := globalNodePortToPodIP[nodeport]
	if !ok {
		return "", false
	}
	return cache.IP, true
}

func PodIPToNodePortIP(podIP string) (nodeIP string, nodeport string, found bool) {
	globalRW.RLock()
	defer globalRW.RUnlock()
	cache, ok := globalPodIPToNodeIPNodePort[podIP]
	if !ok {
		return "", "", false
	}
	return cache.IP, cache.Port, true
}

type cacheIPPort struct {
	IP   string
	Port string
}

var globalRW = sync.RWMutex{}
var globalPodIPToNodeIPNodePort = map[string]cacheIPPort{}
var globalNodePortToPodIP = map[string]cacheIPPort{}

func InstallNodePortSvc(ctx context.Context, K8SService k8s.Services, rf *redisfailoverv1.RedisFailover) error {
	rps, err := K8SService.GetStatefulSetPods(rf.Namespace, fmt.Sprintf("rfr-%s", rf.Name))
	if err != nil {
		return err
	}
	rpsMap := map[string]corev1.Pod{}
	for _, pod := range rps.Items {
		rpsMap[pod.Name] = pod
	}
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: rf.Namespace,
		},
		Spec: corev1.ServiceSpec{
			Ports: []corev1.ServicePort{
				corev1.ServicePort{
					Name:       "redis",
					Protocol:   "TCP",
					Port:       6379,
					TargetPort: intstr.FromInt(6379),
				},
			},
			Type:                     corev1.ServiceTypeNodePort,
			PublishNotReadyAddresses: true,
		},
	}
	for i := int32(0); i < rf.Spec.Redis.Replicas; i++ {
		podName := fmt.Sprintf("rfr-%s-%d", rf.Name, i)
		svc.Name = fmt.Sprintf("rfr-%s-np-%d", rf.Name, i)
		svc.Spec.Selector = map[string]string{
			"statefulset.kubernetes.io/pod-name": podName,
		}
	getSvc:
		service, err := K8SService.GetService(svc.Namespace, svc.Name)
		if err != nil {
			// If no resource we need to create.
			if errors.IsNotFound(err) {
				err := K8SService.CreateService(svc.Namespace, svc)
				if err != nil {
					log.Errorf("Error while create service %v in %v namespace : %v", svc.Name, svc.Namespace, err)
					return err
				}
				goto getSvc
			}
			log.Errorf("Error while get service %v in %v namespace : %v", svc.Name, svc.Namespace, err)
			return err
		}
		if podInfo, ok := rpsMap[podName]; ok {
			if podInfo.Status.PodIP != "" && podInfo.Status.HostIP != "" && len(service.Spec.Ports) != 0 {
				globalRW.Lock()
				globalPodIPToNodeIPNodePort[podInfo.Status.PodIP] = cacheIPPort{
					IP:   podInfo.Status.HostIP,
					Port: strconv.Itoa(int(service.Spec.Ports[0].NodePort)),
				}
				globalNodePortToPodIP[strconv.Itoa(int(service.Spec.Ports[0].NodePort))] = cacheIPPort{
					IP:   podInfo.Status.PodIP,
					Port: "6379",
				}
				globalRW.Unlock()
			} else {
				return fmt.Errorf("pod %s is not ready at %s", podName, rf.Namespace)
			}
		} else {
			return fmt.Errorf("pod %s is not ready at %s", podName, rf.Namespace)
		}
	}
	return nil
}

func CreateOwnerReferences(rf *redisfailoverv1.RedisFailover) []metav1.OwnerReference {
	rfvk := redisfailoverv1.VersionKind(redisfailoverv1.RFKind)
	return []metav1.OwnerReference{
		*metav1.NewControllerRef(rf, rfvk),
	}
}
