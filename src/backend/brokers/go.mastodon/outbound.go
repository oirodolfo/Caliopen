// Copyleft (ɔ) 2019 The Caliopen contributors.
// Use of this source code is governed by a GNU AFFERO GENERAL PUBLIC
// license (AGPL) that can be found in the LICENSE file.

package mastodonbroker

import (
	"errors"
	. "github.com/CaliOpen/Caliopen/src/backend/defs/go-objects"
	log "github.com/Sirupsen/logrus"
	"time"
)

// retrieves a caliopen message from db, builds a DM from it
// gives back DM to worker for it delivers it through mastodon API
// waits for worker ack and finishes saving new state.
func (b *MastodonBroker) ProcessOutDM(order BrokerOrder, worker chan *DMpayload) {

	dmPayload := &DMpayload{
		Response: make(chan MastodonDeliveryAck),
	}

	// 1. build DM
	m, err := b.Store.RetrieveMessage(order.UserId, order.MessageId)
	if err != nil {
		replyError(err, worker)
		return
	}
	if m == nil {
		replyError(errors.New("message from db is empty"), worker)
		return
	}
	if !m.Is_draft {
		replyError(errors.New("message is not a draft"), worker)
		return
	}
	dmPayload.DM, err = MarshalDM(m)
	if err != nil {
		replyError(err, worker)
		return
	}

	// 2. give it back to mastodon worker and wait for response
	select {
	case worker <- dmPayload:
		select {
		case ack := <-dmPayload.Response:
			if ack.Err {
				close(worker)
				return
			}
			// 3. DM has been sent, saving state before ending process
			if err := b.SaveIndexSentDM(order, &ack); err != nil {
				replyError(err, worker)
				return
			}
			worker <- &DMpayload{}
			close(worker)
		case <-time.After(30 * time.Second):
			log.Warn("[ProcessOutDM] 30 second timeout when waiting worker ack")
			close(worker)
			return
		}

	case <-time.After(30 * time.Second):
		log.Warn("[ProcessOutDM] 30 second timeout when giving DM to worker")
		close(worker)
		return
	}
}

func replyError(err error, worker chan *DMpayload) {
	defer close(worker)
	dmPayload := &DMpayload{
		Err: err,
	}
	select {
	case worker <- dmPayload:
		return
	case <-time.After(3 * time.Second):
		return
	}
}
