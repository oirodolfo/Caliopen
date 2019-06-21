// Copyleft (ɔ) 2018 The Caliopen contributors.
// Use of this source code is governed by a GNU AFFERO GENERAL PUBLIC
// license (AGPL) that can be found in the LICENSE file.

package mastodonworker

import (
	"encoding/json"
	"fmt"
	"github.com/CaliOpen/Caliopen/src/backend/brokers/go.mastodon"
	. "github.com/CaliOpen/Caliopen/src/backend/defs/go-objects"
	log "github.com/Sirupsen/logrus"
	"github.com/nats-io/go-nats"
	"github.com/pkg/errors"
	"time"
)

// WorkerMsgHandler handles message coming from idpoller
func (w *Worker) WorkerMsgHandler(msg *nats.Msg) {
	message := BrokerOrder{}
	err := json.Unmarshal(msg.Data, &message)
	if err != nil {
		log.WithError(err).Errorf("Unable to unmarshal message from NATS. Payload was <%s>", string(msg.Data))
		return
	}
	switch message.Order {
	case noPendingJobErr:
		return
	case "sync":
		log.Infof("received sync order for remote mastodon ID %s", message.IdentityId)
		if accountWorker := w.getOrCreateHandler(message.UserId, message.IdentityId); accountWorker != nil {
			select {
			case accountWorker.AccountDesk <- PollDM:
				log.Infof("[DMmsgHandler] ordering to pollDM for remote %s (user %s)", message.IdentityId, message.UserId)
			case <-time.After(30 * time.Second):
				log.Warnf("[DMmsgHandler] worker's desk is full for remote %s (user %s)", message.IdentityId, message.UserId)
			}
		} else {
			log.Warnf("[DMmsgHandler] failed to get a worker for remote %s (user %s)", message.IdentityId, message.UserId)
			w.natsReplyError(msg, errors.New("[DMmsgHandler] failed to get a worker"))
		}
	case "reload_worker":
		log.Infof("received reload_worker order for remote mastodon ID %s", message.IdentityId)
		//TODO: order to force refreshing cache data for an account
	case "add_worker":
		log.Infof("received add_worker order for remote mastodon ID %s", message.IdentityId)
		accountWorker := w.getOrCreateHandler(message.UserId, message.IdentityId)
		if accountWorker == nil {
			log.WithError(err).Warnf("[WorkerMsgHandler] failed to create new worker for remote %s (user %s)", message.IdentityId, message.UserId)
			w.natsReplyError(msg, errors.New("[DMmsgHandler] failed to get a worker"))
		}
	case "remove_worker":
		log.Infof("received remove_worker order for remote mastodon ID %s", message.IdentityId)
		// TODO
	}
}

// DMmsgHandler handles messages coming on topic dedicated to DM management
func (w *Worker) DMmsgHandler(msg *nats.Msg) {
	message := BrokerOrder{}
	err := json.Unmarshal(msg.Data, &message)
	if err != nil {
		log.WithError(err).Errorf("Unable to unmarshal message from NATS. Payload was <%s>", string(msg.Data))
		return
	}
	switch message.Order {
	case "deliver":
		if accountWorker := w.getOrCreateHandler(message.UserId, message.IdentityId); accountWorker != nil {
			com := mastodonbroker.NatsCom{
				Order: message,
				Ack:   make(chan *DeliveryAck),
			}
			select {
			case accountWorker.broker.Connectors.Egress <- com:
				log.Infof("[DMmsgHandler] sending DM for remote %s (user %s)", message.IdentityId, message.UserId)
				// non-blocking wait for delivery ack
				go func(com mastodonbroker.NatsCom) {
					select {
					case resp := <-com.Ack:
						if resp.Err {
							w.natsReplyError(msg, errors.New(resp.Response))
						} else {
							ack := DeliveryAck{
								Err:      false,
								Response: "OK",
							}
							json_resp, _ := json.Marshal(ack)
							w.NatsConn.Publish(msg.Reply, json_resp)
						}
					case <-time.After(30 * time.Second):
						w.natsReplyError(msg, errors.New("[DMmsgHandler] timeout waiting broker delivery ack"))
					}
				}(com)
			case <-time.After(30 * time.Second):
				log.Warnf("[DMmsgHandler] worker's Egress connectors is full for remote %s (user %s)", message.IdentityId, message.UserId)
				w.natsReplyError(msg, errors.New("[DMmsgHandler] failed to get a worker"))
			}
		} else {
			w.natsReplyError(msg, errors.New("[DMmsgHandler] failed to get a worker"))
		}
	default:
		w.natsReplyError(msg, errors.New("not implemented"))
	}
}

func (w *Worker) natsReplyError(msg *nats.Msg, err error) {
	log.WithError(err).Warnf("mastodon broker [outbound] : error when processing incoming nats message : %v", *msg)

	ack := DeliveryAck{
		Err:      true,
		Response: fmt.Sprintf("failed to send message with error « %s » ", err), //TODO
	}

	json_resp, _ := json.Marshal(ack)
	w.NatsConn.Publish(msg.Reply, json_resp)
}
