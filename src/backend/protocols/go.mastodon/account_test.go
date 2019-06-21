// Copyleft (ɔ) 2019 The Caliopen contributors.
// Use of this source code is governed by a GNU AFFERO GENERAL PUBLIC
// license (AGPL) that can be found in the LICENSE file.

package mastodonworker

import (
	"github.com/CaliOpen/Caliopen/src/backend/main/go.backends/backendstest"
	"testing"
	"time"
)

func TestNewAccountHandler(t *testing.T) {
	w, s, err := initWorkerTest()
	if err != nil {
		t.Error(err)
		return
	}
	defer s.Shutdown()
	ah, err := NewAccountHandler(backendstest.EmmaTommeUserId, "b91f0fa8-17a2-4729-8a5a-5ff58ee5c121", *w)
	if err != nil {
		t.Error(err)
		return
	}
	if ah.broker == nil {
		t.Error("expected account handler with broker initialized, got broker==nil")
	}
	if ah.twitterClient == nil {
		t.Error("expected account handler with twitterClient initialized, got client==nil")
	}
	if ah.userAccount.twitterID != "000000" {
		t.Errorf("expected userAccount with twitterID == 000000, got %s", ah.userAccount.twitterID)
	}

	// test that closing broker's connectors will kill accountHandler
	go ah.Start()
	time.Sleep(100 * time.Millisecond)
	close(ah.broker.Connectors.Egress)
	time.Sleep(100 * time.Millisecond)
	if _, ok := <-ah.AccountDesk; ok {
		t.Error("expected handler's accountDesk to be closed, still open")
	}
	if _, ok := <-ah.broker.Connectors.Halt; ok {
		t.Error("expected handler.broker's connectors.halt to be closed, still open")
	}
	if _, ok := <-ah.broker.Connectors.Egress; ok {
		t.Error("expected handler.broker's connectors.Egress to be closed, still open")
	}
	if len(w.AccountHandlers) > 0 {
		t.Errorf("expected empty AccountHandlers map, got len=%d", len(w.AccountHandlers))
	}
}

func TestAccountHandler_Start(t *testing.T) {
	w, s, err := initWorkerTest()
	if err != nil {
		t.Error(err)
		return
	}
	defer s.Shutdown()
	ah, err := NewAccountHandler(backendstest.EmmaTommeUserId, "b91f0fa8-17a2-4729-8a5a-5ff58ee5c121", *w)
	if err != nil {
		t.Error(err)
		return
	}
	go ah.Start()
}

func TestAccountHandler_Stop(t *testing.T) {
	w, s, err := initWorkerTest()
	if err != nil {
		t.Error(err)
		return
	}
	defer s.Shutdown()
	ah, err := NewAccountHandler(backendstest.EmmaTommeUserId, "b91f0fa8-17a2-4729-8a5a-5ff58ee5c121", *w)
	if err != nil {
		t.Error(err)
		return
	}

	go ah.Start()
	time.Sleep(100 * time.Millisecond)
	ah.Stop()
	time.Sleep(100 * time.Millisecond)
	if _, ok := <-ah.AccountDesk; ok {
		t.Error("expected handler's accountDesk to be closed, still open")
	}
	if _, ok := <-ah.broker.Connectors.Halt; ok {
		t.Error("expected handler.broker's connectors.halt to be closed, still open")
	}
	if _, ok := <-ah.broker.Connectors.Egress; ok {
		t.Error("expected handler.broker's connectors.Egress to be closed, still open")
	}
}
