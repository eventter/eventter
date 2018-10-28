package mq

import (
	"fmt"
	"strconv"

	"github.com/hashicorp/memberlist"
	"github.com/hashicorp/raft"
)

type RaftMembersDelegate struct {
	r *raft.Raft
}

func NewRaftMembersDelegate(r *raft.Raft) *RaftMembersDelegate {
	return &RaftMembersDelegate{r}
}

func (d *RaftMembersDelegate) NotifyJoin(node *memberlist.Node) {
	if d.r.State() != raft.Leader {
		return
	}

	go func() {
		fmt.Println("add node:", node.Name, node.Addr)

		future := d.r.AddVoter(
			raft.ServerID(node.Name),
			raft.ServerAddress(node.Addr.String()+":"+strconv.Itoa(int(node.Port))),
			0,
			0,
		)
		if err := future.Error(); err != nil {
			fmt.Println("add node error:", err)
		} else {
			fmt.Println("add node success:", node.Name)
		}
	}()
}

func (d *RaftMembersDelegate) NotifyLeave(node *memberlist.Node) {
	//if d.r.State() != raft.Leader {
	//	return
	//}
	//
	//fmt.Println("remove node:", node.Name, node.Addr)
	//
	//future := d.r.RemoveServer(raft.ServerID(node.Name), 0, 0)
	//if err := future.Error(); err != nil {
	//	fmt.Println("remove node error:", err)
	//} else {
	//	fmt.Println("remove node success:", node.Name)
	//}
}

func (d *RaftMembersDelegate) NotifyUpdate(node *memberlist.Node) {
}
