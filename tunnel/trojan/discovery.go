package trojan

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/p4gefau1t/trojan-go/log"
	"go.etcd.io/etcd/clientv3"
)

//the detail of service 定义服务结构，唯一id加ip地址
type ServiceInfo struct {
	ID uint64
	IP string
}

type Node struct {
	Name    string
	Info    ServiceInfo
	stop    chan error
	leaseid clientv3.LeaseID
	client  *clientv3.Client
}

func NewNode(name string, info ServiceInfo, endpoints []string) (*Node, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 2 * time.Second,
	})

	if err != nil {
		log.Fatal(err)
		return nil, err
	}

	return &Node{
		Name:   name,
		Info:   info,
		stop:   make(chan error),
		client: cli,
	}, err
}

func (s *Node) Start() error {
	ch, err := s.keepAlive()
	if err != nil {
		log.Fatal(err)
		return err
	}

	for {
		select {
		case err := <-s.stop:
			s.revoke()
			return err
		case <-s.client.Ctx().Done():
			return errors.New("server closed")
		case ka, ok := <-ch:
			if !ok {
				log.Info("keep alive channel closed")
				s.revoke()
				return nil
			} else {
				log.Info("Recv reply from service: %s, ttl:%d", s.Name, ka.TTL)
			}
		}
	}
}

func (s *Node) Stop() {
	s.stop <- nil
}

func (s *Node) keepAlive() (<-chan *clientv3.LeaseKeepAliveResponse, error) {
	info := &s.Info
	key := "services/" + s.Name
	value, _ := json.Marshal(info)

	// minimum lease TTL is 5-second
	resp, err := s.client.Grant(context.TODO(), 5)
	if err != nil {
		log.Fatal(err)
		return nil, err
	}

	_, err = s.client.Put(context.TODO(), key, string(value), clientv3.WithLease(resp.ID))
	if err != nil {
		log.Fatal(err)
		return nil, err
	}
	s.leaseid = resp.ID

	return s.client.KeepAlive(context.TODO(), resp.ID)
}

func (s *Node) revoke() error {
	_, err := s.client.Revoke(context.TODO(), s.leaseid)
	if err != nil {
		log.Fatal(err)
	}
	log.Info("servide:%s stop\n", s.Name)
	return err
}
