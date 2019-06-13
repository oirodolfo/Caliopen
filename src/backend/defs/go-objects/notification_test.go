package objects

import (
	"github.com/gocql/gocql"
	"github.com/satori/go.uuid"
	"github.com/tidwall/gjson"
	"testing"
)

func TestNotification_UnmarshalCQLMap(t *testing.T) {
	input := map[string]interface{}{
		"user_id":  gocql.UUID(uuid.FromStringOrNil("63ab7904-c416-4f1a-9652-3de82e4fd1f1")),
		"notif_id": gocql.UUID(uuid.FromStringOrNil("63ab7904-c416-1f1a-9652-3de82e4fd1f1")),
		"body": []byte(`{"elements": [
		{"body": {"contact_id": "63ab7904-c416-4f1a-9652-3de82e4fd1f1", "status": "imported"}},
		{"body": {"contact_id": "63ab7904-c416-4f1a-9652-3de82e4fd1f1", "status": "error", "error_msg": "something went wrong"}},
		{"body": {"contact_id": "63ab7904-c416-4f1a-9652-3de82e4fd1f1", "status": "ignored"}}
		],
		"size": 33}`),
		"emitter": "contacts",
		"type":    "import_result",
	}
	notif := new(Notification)
	notif.UnmarshalCQLMap(input)
	if len(notif.Children) != 3 {
		t.Errorf("expected Notification with 3 children, got %d", len(notif.Children))
	}
	if !gjson.Valid(notif.Children[0].Body) {
		t.Error("expected a valid json in Notification.Children[0].Body, got gjson.Valid == false")
	}
	if gjson.Get(notif.Children[0].Body, "contact_id").Str != "63ab7904-c416-4f1a-9652-3de82e4fd1f1" {
		t.Errorf("expected Notification.Children[0].Body.contact_id == \"63ab7904-c416-4f1a-9652-3de82e4fd1f1\", got %s", gjson.Get(notif.Children[0].Body, "body.contact_id").Str)
	}
	if notif.Emitter != "contacts" {
		t.Errorf("expected Notifiaction.Emitter == \"contacts\", got %s", notif.Emitter)
	}
	if notif.ChildrenCount != 33 {
		t.Errorf("expected Notification.ChildrenCount == 33, got %d", notif.ChildrenCount)
	}
}
