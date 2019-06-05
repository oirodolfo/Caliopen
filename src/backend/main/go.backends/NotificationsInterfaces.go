// Copyleft (ɔ) 2017 The Caliopen contributors.
// Use of this source code is governed by a GNU AFFERO GENERAL PUBLIC
// license (AGPL) that can be found in the LICENSE file.

package backends

import (
	. "github.com/CaliOpen/Caliopen/src/backend/defs/go-objects"
	"time"
)

type NotificationsStore interface {
	CreateMessage(msg *Message) error
	UserByUsername(username string) (user *User, err error) // to retrieve admin user
	RetrieveLocalsIdentities(user_id string) (identities []UserIdentity, err error)
	PutNotificationInQueue(*Notification) error
	NotificationsByTime(userId string, from, to time.Time) ([]Notification, error)
	NotificationsByID(userId, from, to string) ([]Notification, error)
	RetrieveNotification(userId, notificationId string) (Notification, error)
	DeleteNotifications(userId string, until time.Time) error
	DeleteNotification(userId, notificationId string) error
}

type NotificationsIndex interface {
	CreateMessage(user *UserInfo, msg *Message) error
}
