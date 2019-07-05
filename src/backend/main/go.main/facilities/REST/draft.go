// Copyleft (ɔ) 2017 The Caliopen contributors.
// Use of this source code is governed by a GNU AFFERO GENERAL PUBLIC
// license (AGPL) that can be found in the LICENSE file.

package REST

import (
	"encoding/json"
	"errors"
	"fmt"
	. "github.com/CaliOpen/Caliopen/src/backend/defs/go-objects"
	"github.com/CaliOpen/Caliopen/src/backend/main/go.main/messages"
	log "github.com/Sirupsen/logrus"
	"time"
)

func (rest *RESTfacility) SendDraft(user_info *UserInfo, msg_id string) (msg *Message, err error) {
	const nats_order = "deliver"
	var order BrokerOrder
	draft, draftErr := rest.store.RetrieveMessage(user_info.User_id, msg_id)
	if draftErr != nil {
		log.WithError(draftErr).Info("[SendDraft] failed to retrieve draft from store")
		return nil, errors.New("draft not found")
	}
	// resolve sender's address protocol for selecting natsTopics accordingly
	protocol, resolvErr := rest.ResolveSenderProtocol(draft)
	if resolvErr != nil {
		log.WithError(resolvErr).Info("[SendDraft] failed to resolve sender's protocol")
		return nil, errors.New("unknown protocol for sender")
	}
	// Ensure participants and hash lookups are filled accordingly (they should)
	err = rest.store.UpsertDiscussionLookups(draft.User_id, draft.Participants)
	if err != nil {
		log.WithError(err).Error("[SendDraft] failed to create discussion's lookups")
		return nil, err
	}

	var natsTopic string
	switch protocol {
	case EmailProtocol, ImapProtocol:
		natsTopic = Nats_outIMAP_topicKey
		order = BrokerOrder{
			Order:      nats_order,
			MessageId:  msg_id,
			UserId:     user_info.User_id,
			IdentityId: draft.UserIdentities[0].String(), // handle one identity only for now
		}
	case SmtpProtocol:
		natsTopic = Nats_outSMTP_topicKey
		order = BrokerOrder{
			Order:      nats_order,
			MessageId:  msg_id,
			UserId:     user_info.User_id,
			IdentityId: draft.UserIdentities[0].String(), // handle one identity only for now
		}
	case TwitterProtocol:
		natsTopic = Nats_outTwitter_topicKey
		order = BrokerOrder{
			Order:      nats_order,
			MessageId:  msg_id,
			UserId:     user_info.User_id,
			IdentityId: draft.UserIdentities[0].String(), // handle one identity for now
		}
	case MastodonProtocol:
		natsTopic = Nats_outMastodon_topicKey
		order = BrokerOrder{
			Order:      nats_order,
			MessageId:  msg_id,
			UserId:     user_info.User_id,
			IdentityId: draft.UserIdentities[0].String(), // handle one identity for now
		}
	default:
		return nil, fmt.Errorf("[SendDraft] no handler for <%s> protocol", protocol)
	}
	natsMessage, e := json.Marshal(order)
	if e != nil {
		log.WithError(e).Info("[SendDraft] failed to build nats message")
		return nil, errors.New("[SendDraft] failed to build nats message")
	}
	rep, err := rest.nats_conn.Request(rest.natsTopics[natsTopic], natsMessage, 30*time.Second)
	if err != nil {
		log.WithError(err).Warn("[RESTfacility]: SendDraft error (1)")
		if rest.nats_conn.LastError() != nil {
			log.WithError(rest.nats_conn.LastError()).Warn("[RESTfacility]: SendDraft error")
			return nil, err
		}
		return nil, err
	}

	var reply DeliveryAck
	err = json.Unmarshal(rep.Data, &reply)
	if err != nil {
		log.WithError(err).Warn("[RESTfacility]: SendDraft error (2)")
		return nil, err
	}
	if reply.Err {
		log.Warn("[RESTfacility]: SendDraft error (3)")
		return nil, errors.New(reply.Response)
	}
	msg, err = rest.store.RetrieveMessage(user_info.User_id, msg_id)
	if err != nil {
		return nil, err
	}
	messages.SanitizeMessageBodies(msg)
	(*msg).Body_excerpt = messages.ExcerptMessage(*msg, 200, true, true)
	return msg, err
}

// ResolveSenderProtocol returns outbound protocol to use for sending draft by resolving draft's sender identity
func (rest *RESTfacility) ResolveSenderProtocol(draft *Message) (string, error) {
	firstIdentity, err := rest.RetrieveUserIdentity(draft.User_id.String(), draft.UserIdentities[0].String(), false) // handle one identity only for now
	if err != nil {
		return "", err
	}
	return firstIdentity.Protocol, nil
}
