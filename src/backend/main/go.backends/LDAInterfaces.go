// Copyleft (ɔ) 2017 The Caliopen contributors.
// Use of this source code is governed by a GNU AFFERO GENERAL PUBLIC
// license (AGPL) that can be found in the LICENSE file.

package backends

import (
	. "github.com/CaliOpen/Caliopen/src/backend/defs/go-objects"
	"io"
	"time"
)

//Local Delivery Agent storage interface
type LDAStore interface {
	Close()
	RetrieveMessage(user_id, msg_id string) (msg *Message, err error)
	GetUsersForLocalMailRecipients([]string) ([][]UUID, error) // returns a list of tuples ([user_id, identity_id]) of **local** users found for given recipients list. No deduplicate.
	GetSettings(user_id string) (settings *Settings, err error)
	CreateMessage(msg *Message) error

	StoreRawMessage(msg RawMessage) (err error)
	GetRawMessage(raw_message_id string) (raw_message RawMessage, err error)
	SetDeliveredStatus(raw_msg_id string, delivered bool) error
	UpdateMessage(msg *Message, fields map[string]interface{}) error // 'fields' are the struct fields names that have been modified
	SeekMessageByExternalRef(userID, externalMessageID, identityID string) (UUID, error)

	LookupContactsByIdentifier(user_id, address, kind string) (contact_ids []string, err error)

	GetAttachment(uri string) (file io.Reader, err error)
	DeleteAttachment(uri string) error
	AttachmentExists(uri string) bool

	RetrieveUserIdentity(userId, identityId string, withCredentials bool) (*UserIdentity, error)
	UpdateUserIdentity(userIdentity *UserIdentity, fields map[string]interface{}) error
	RetrieveUser(user_id string) (user *User, err error)
	UpdateRemoteInfosMap(userId, remoteId string, infos map[string]string) error
	RetrieveRemoteInfosMap(userId, remoteId string) (infos map[string]string, err error)
	TimestampRemoteLastCheck(userId, remoteId string, time ...time.Time) error

	RetrieveProvider(name, instance string) (*Provider, CaliopenError)
}

type LDAIndex interface {
	Close()
	CreateMessage(user *UserInfo, msg *Message) error
	UpdateMessage(user *UserInfo, msg *Message, fields map[string]interface{}) error
}
