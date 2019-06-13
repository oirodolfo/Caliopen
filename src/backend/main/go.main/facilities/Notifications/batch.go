package Notifications

import (
	"encoding/json"
	"errors"
	. "github.com/CaliOpen/Caliopen/src/backend/defs/go-objects"
	"github.com/satori/go.uuid"
	"strconv"
	"strings"
	"sync"
)

type BatchNotification struct {
	emitter                string
	locker                 *sync.Mutex
	notifications          []Notification
	notificationsCount     int
	notificationsThreshold int
}

const notificationsThreshold = 20

type BatchNotifier interface {
	Add(Notification)
	Save(notifier Notifiers, reference, ttl string)
}

func NewBatch(emitter string) *BatchNotification {
	return &BatchNotification{
		emitter:                emitter,
		locker:                 new(sync.Mutex),
		notificationsThreshold: notificationsThreshold,
	}
}

func (bn *BatchNotification) Add(n Notification) {
	bn.locker.Lock()
	bn.notificationsCount++
	if bn.notificationsCount <= bn.notificationsThreshold {
		bn.notifications = append(bn.notifications, n)
	}
	bn.locker.Unlock()
}

// Save aggregates notifications into a single one with sub-notifications into its Body as a json array
// then saves this notification in user's cassandra queue
func (bn *BatchNotification) Save(notifier Notifiers, reference, ttl string) {
	notif, err := bn.aggregate(reference, ttl)
	if err == nil {
		notifier.ByNotifQueue(&notif)
	}
}

// aggregate flatten notifications into a single Notification
// children are embedded in Notification.Body as a json array if they are less than batchThreshold
// otherwise, only children_count is written
func (bn *BatchNotification) aggregate(reference, ttl string) (Notification, error) {
	if len(bn.notifications) == 0 {
		return Notification{}, errors.New("[BatchNotifier] elements is empty")
	}
	notif := Notification{
		Emitter:       bn.emitter,
		NotifId:       UUID(uuid.NewV1()),
		Reference:     reference,
		TTLcode:       ttl,
		Type:          bn.notifications[0].Type,
		User:          bn.notifications[0].User,
		ChildrenCount: bn.notificationsCount,
	}
	children := make([]NotificationModel, 0, len(bn.notifications))
	if bn.notificationsCount <= bn.notificationsThreshold {
		for _, n := range bn.notifications {
			if notif.User == nil || n.User == nil || (notif.User.UserId.String() != n.User.UserId.String()) {
				return Notification{}, errors.New("[BatchNotifier] can't aggregate notifications : inconsistent user ids within notifications slice")
			}
			children = append(children, NotificationModel{
				Body: n.Body,
			})
		}
	}
	jChildren, err := json.Marshal(children)
	if err != nil {
		return Notification{}, err
	}
	body := strings.Builder{}
	body.WriteString(`{"size":`)
	body.WriteString(strconv.Itoa(bn.notificationsCount) + ",")
	body.WriteString(`"elements":`)
	body.WriteString(string(jChildren))
	body.WriteString(`}`)
	notif.Body = body.String()
	return notif, nil
}
