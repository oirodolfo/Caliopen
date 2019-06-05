// Copyleft (ɔ) 2019 The Caliopen contributors.
// Use of this source code is governed by a GNU AFFERO GENERAL PUBLIC
// license (AGPL) that can be found in the LICENSE file.

package backendstest

import (
	"errors"
	. "github.com/CaliOpen/Caliopen/src/backend/defs/go-objects"
	"time"
)

type NotificationsStore struct{}
type NotificationsIndex struct{}

func GetNotificationsBackends() (store NotificationsStore, index NotificationsIndex) {
	return NotificationsStore{}, NotificationsIndex{}
}

func (ns NotificationsStore) CreateMessage(msg *Message) error {
	return errors.New("test interface not implemented")
}
func (ns NotificationsStore) UserByUsername(username string) (user *User, err error) {
	return UserByUsername(username)
}
func (ns NotificationsStore) RetrieveLocalsIdentities(userId string) (identities []UserIdentity, err error) {
	return RetrieveLocalsIdentities(userId)
}
func (ns NotificationsStore) PutNotificationInQueue(*Notification) error {
	return errors.New("test interface not implemented")
}

func (ns NotificationsStore) DeleteNotifications(userId string, until time.Time) error {
	return errors.New("test interface not implemented")
}

func (ns NotificationsStore) NotificationsByTime(userId string, from, to time.Time) ([]Notification, error) {

	return []Notification{}, errors.New("test interface not implemented")
}
func (ns NotificationsStore) NotificationsByID(userId, from, to string) ([]Notification, error) {

	return []Notification{}, errors.New("test interface not implemented")
}
func (ns NotificationsStore) RetrieveNotification(userId, notificationId string) (Notification, error) {

	return Notification{}, errors.New("test interface not implemented")
}
func (ns NotificationsStore) DeleteNotification(userId, notificationId string) error {
	return errors.New("test interface not implemented")
}

func (ni NotificationsIndex) CreateMessage(user *UserInfo, msg *Message) error {
	return errors.New("test interface not implemented")
}
