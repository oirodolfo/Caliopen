package Notifications

import (
	. "github.com/CaliOpen/Caliopen/src/backend/defs/go-objects"
	"github.com/satori/go.uuid"
	"github.com/tidwall/gjson"
	"sync"
	"testing"
)

func TestBatchNotification_aggregate(t *testing.T) {
	bn := BatchNotification{
		emitter:                "test",
		locker:                 new(sync.Mutex),
		notificationsCount:     3,
		notificationsThreshold: notificationsThreshold,
		notifications: []Notification{
			{Body: `{"contact_id": "63ab7904-c416-4f1a-9652-3de82e4fd1f1", "status": "imported"}`,
				User: &User{UserId: UUID(uuid.FromStringOrNil("63ab7904-c416-4f1a-9652-3de82e4fd1f1"))}},
			{Body: `{"contact_id": "63ab7904-c416-4f1a-9652-3de82e4fd1f1", "status": "error", "error_msg": "something went wrong"}`,
				User: &User{UserId: UUID(uuid.FromStringOrNil("63ab7904-c416-4f1a-9652-3de82e4fd1f1"))}},
			{Body: `{"contact_id": "63ab7904-c416-4f1a-9652-3de82e4fd1f1", "status": "ignored"}`,
				User: &User{UserId: UUID(uuid.FromStringOrNil("63ab7904-c416-4f1a-9652-3de82e4fd1f1"))}},
		},
	}

	n, err := bn.aggregate("ref", LongLived)
	if err != nil {
		t.Error(err)
	} else {
		if !gjson.Valid(n.Body) {
			t.Error("expected a valid json in body, gjson reported invalid")
		} else {
			result := gjson.Get(n.Body, "children_count")
			if !result.Exists() {
				t.Error("expected property 'children_count' in notification body but gjson can't find it")
			} else {
				if result.Int() != 3 {
					t.Errorf("expected body.children_count = 3, got %d", result.Int())
				}
			}
			result = gjson.Get(n.Body, "children")
			if !result.Exists() || !result.IsArray() {
				t.Error("expected body.children to be an array, gjson reported it doesn't exist or not an array")
			} else {
				childrenLength := gjson.Get(n.Body, "children.#")
				if childrenLength.Num != 3.0 {
					t.Errorf("expected an array with 3 obj in body.children, got %f", childrenLength.Num)
				}
				lastChildStatus := gjson.Get(n.Body, "children.2.body.status")
				if lastChildStatus.Str != "ignored" {
					t.Errorf("expected last children.2.body.status == 'ignored', got %s", lastChildStatus.Raw)
				}
			}
		}
	}
}
