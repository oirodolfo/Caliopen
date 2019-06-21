// Copyleft (ɔ) 2019 The Caliopen contributors.
// Use of this source code is governed by a GNU AFFERO GENERAL PUBLIC
// license (AGPL) that can be found in the LICENSE file.

package mastodonworker

import (
	"encoding/json"
	. "github.com/CaliOpen/Caliopen/src/backend/defs/go-objects"
	"github.com/CaliOpen/Caliopen/src/backend/main/go.backends/backendstest"
	idpoller "github.com/CaliOpen/Caliopen/src/backend/workers/go.remoteIDs"
	"github.com/nats-io/go-nats"
	"github.com/satori/go.uuid"
	"testing"
	"time"
)

func TestWorker_WorkerMsgHandler(t *testing.T) {
	w, s, err := initWorkerTest()
	if err != nil {
		t.Error(err)
		return
	}
	defer s.Shutdown()

	noJobMsg := nats.Msg{
		Subject: "test",
		Reply:   "testMsgReply",
		Data:    []byte(`{"order":"no pending job"}`),
	}
	// 'no job' message should trigger an immediate return
	c := make(chan struct{})
	go func() {
		w.WorkerMsgHandler(&noJobMsg)
		close(c)
	}()
	select {
	case <-c:
	case <-time.After(100 * time.Millisecond):
		t.Error("timeout waiting for no job return")
	}

	gotReply := false
	w.NatsConn.Subscribe("testMsgReply", func(msg *nats.Msg) {
		gotReply = true
	})
	// test 'add_worker' that should fail
	fakeID := uuid.NewV4().String()
	job := idpoller.Job{
		Worker: "twitter",
		Order: BrokerOrder{
			MessageId:  fakeID,
			Order:      "add_worker",
			IdentityId: fakeID,
			UserId:     fakeID,
		},
	}
	data, _ := json.Marshal(job.Order)
	syncMsg := nats.Msg{
		Subject: "test",
		Reply:   "testMsgReply",
		Data:    data,
	}
	w.WorkerMsgHandler(&syncMsg)
	time.Sleep(time.Second)
	if !gotReply {
		t.Error("expected worker replied an error for bad 'add_worker', got nothing on topic")
	}

	// test 'sync' order that should fail
	gotReply = false
	job = idpoller.Job{
		Worker: "twitter",
		Order: BrokerOrder{
			MessageId:  fakeID,
			Order:      "sync",
			IdentityId: fakeID,
			UserId:     fakeID,
		},
	}
	data, _ = json.Marshal(job.Order)
	syncMsg = nats.Msg{
		Subject: "test",
		Reply:   "testMsgReply",
		Data:    data,
	}
	w.WorkerMsgHandler(&syncMsg)
	time.Sleep(time.Second)
	if !gotReply {
		t.Error("expected worker replied an error for bad 'sync', got nothing on topic")
	}

	// test 'add_worker' with a valid remote
	gotReply = false
	job = idpoller.Job{
		Worker: "twitter",
		Order: BrokerOrder{
			MessageId:  fakeID,
			Order:      "add_worker",
			IdentityId: "b91f0fa8-17a2-4729-8a5a-5ff58ee5c121",
			UserId:     backendstest.EmmaTommeUserId,
		},
	}
	data, _ = json.Marshal(job.Order)
	syncMsg = nats.Msg{
		Subject: "test",
		Reply:   "testMsgReply",
		Data:    data,
	}
	w.NatsConn.Subscribe("testMsgReply", func(msg *nats.Msg) {
		t.Errorf("expected no reply for valid 'add_worker' order , got %s", msg.Data)
	})
	w.WorkerMsgHandler(&syncMsg)

	// test 'sync' order with a valid remote
	job = idpoller.Job{
		Worker: "twitter",
		Order: BrokerOrder{
			MessageId:  fakeID,
			Order:      "sync",
			IdentityId: "b91f0fa8-17a2-4729-8a5a-5ff58ee5c121",
			UserId:     backendstest.EmmaTommeUserId,
		},
	}
	data, _ = json.Marshal(job.Order)
	syncMsg = nats.Msg{
		Subject: "test",
		Reply:   "testMsgReply",
		Data:    data,
	}
	w.NatsConn.Subscribe("testMsgReply", func(msg *nats.Msg) {
		t.Errorf("expected no reply for valid 'sync' order , got %s", msg.Data)
	})
	w.WorkerMsgHandler(&syncMsg)
}

func TestWorker_DMmsgHandler(t *testing.T) {
	w, s, err := initWorkerTest()
	if err != nil {
		t.Error(err)
		return
	}
	defer s.Shutdown()

	gotReply := false
	w.NatsConn.Subscribe("testMsgReply", func(msg *nats.Msg) {
		gotReply = true
	})

	// test 'deliver' that should fail
	fakeID := uuid.NewV4().String()
	job := idpoller.Job{
		Worker: "twitter",
		Order: BrokerOrder{
			MessageId:  fakeID,
			Order:      "deliver",
			IdentityId: fakeID,
			UserId:     fakeID,
		},
	}
	data, _ := json.Marshal(job.Order)
	deliverMsg := nats.Msg{
		Subject: "test",
		Reply:   "testMsgReply",
		Data:    data,
	}
	w.DMmsgHandler(&deliverMsg)
	time.Sleep(time.Second)
	if !gotReply {
		t.Error("expected worker replied an error for bad 'deliver', got nothing on topic")
	}

	// test 'deliver' with a valid remote
	job = idpoller.Job{
		Worker: "twitter",
		Order: BrokerOrder{
			MessageId:  fakeID,
			Order:      "deliver",
			IdentityId: "b91f0fa8-17a2-4729-8a5a-5ff58ee5c121",
			UserId:     backendstest.EmmaTommeUserId,
		},
	}
	data, _ = json.Marshal(job.Order)
	deliverMsg = nats.Msg{
		Subject: "test",
		Reply:   "testMsgReply",
		Data:    data,
	}
	w.NatsConn.Subscribe("testMsgReply", func(msg *nats.Msg) {
		// should return an error because deliver process is not mocked for now
		gotReply = true
		var reply DeliveryAck
		err = json.Unmarshal(msg.Data, &reply)
		if !reply.Err {
			t.Error("expected 'deliver' order to trigger an error, got reply.Err == false")
		}
	})
	w.DMmsgHandler(&deliverMsg)
	time.Sleep(time.Second)
	if !gotReply {
		t.Error("expected worker replied an error for valid 'deliver', got nothing on topic")
	}
}
