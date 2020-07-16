package client

import (
	"context"
	"fmt"
	"hash/crc32"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/subiz/header"
	pb "github.com/subiz/header/noti5"
	"google.golang.org/grpc"
)

// Client helps you send message to notify service easier
type Client struct {
	sync.Mutex
	clients []header.Noti5ServiceClient

	service  string // eg: noti:21021
	maxNodes int
}

// NewClient creates a new Client service
// Notes: we don't connect to all noti5 service right away because it
// may blocks start up flow
func NewClient(service string, maxNodes int) *Client {
	return &Client{
		service:  service,
		maxNodes: maxNodes,
		clients:  make([]header.Noti5ServiceClient, maxNodes, maxNodes),
	}
}

// Noti delivers a payload to the correct noti5 service
func (me *Client) Noti(accid string, noti *pb.PushNoti) error {
	client, err := me.getClient(accid)
	if err != nil {
		return err
	}
	ctx := context.Background()

	noti.AccountId = &accid
	_, err = client.Noti(ctx, noti)
	return err
}

// getClient returns the correct noti5 client for an account ID
// the returned client must be ready to be used
func (me *Client) getClient(accid string) (header.Noti5ServiceClient, error) {
	no := int(crc32.ChecksumIEEE([]byte(accid))) % me.maxNodes

	me.Lock()
	if me.clients[no] != nil {
		me.Unlock()
		return me.clients[no], nil
	}

	defer me.Unlock()

	parts := strings.SplitN(me.service, ":", 2)
	name, port := parts[0], parts[1]
	// address: [pod name] + "." + [service name] + ":" + [pod port]
	conn, err := dialGrpc(name + "-" + strconv.Itoa(no) + "." + name + ":" + port)
	if err != nil {
		fmt.Println("unable to connect to noti5 service", err)
		return nil, err
	}
	me.clients[no] = header.NewNoti5ServiceClient(conn)
	return me.clients[no], nil
}

func dialGrpc(service string) (*grpc.ClientConn, error) {
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithInsecure())
	// Enabling WithBlock tells the client to not give up trying to find a server
	opts = append(opts, grpc.WithBlock())
	// However, we're still setting a timeout so that if the server takes too long, we still give up
	opts = append(opts, grpc.WithTimeout(120*time.Second))
	return grpc.Dial(service, opts...)
}
