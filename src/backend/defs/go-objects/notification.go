/*
 * // Copyleft (ɔ) 2018 The Caliopen contributors.
 * // Use of this source code is governed by a GNU AFFERO GENERAL PUBLIC
 * // license (AGPL) that can be found in the LICENSE file.
 */

package objects

import (
	"github.com/gocql/gocql"
	"github.com/satori/go.uuid"
	"github.com/tidwall/gjson"
)

type Notification struct {
	Body            string      // could be a simple word or a more complex structure like a json, depending of the notification.
	Emitter         string      // backend entity that's emitting the message
	NotifId         UUID        // uuid v1, including a timestamp
	InternalPayload interface{} // placeholder to put objects needed to build/fulfil notification. Will not be emitted and/or saved.
	Reference       string      // (optional) a reference number previously sent by frontend to link current notification to a previous action/event
	Type            string      // a single word to describe message's type and give indication of importance level (event, info, feedback, warning, teaser, error, alert, etc.)
	TTLcode         string      // chars to pickup default duration into notification_ttl table.
	User            *User       // only userId will be exported
	Children        []Notification
	ChildrenCount   int // in case they are too many children, children will be empty but not ChildrenCount
}

// model to queue a notification in cassandra or marshal one to json
type NotificationModel struct {
	// PRIMARY KEYS (user_id, notif_id)
	Body      string              `cql:"body"         json:"body,omitempty"    formatter:"raw"` // our bespoke jsonMarshaler will not escape this string, thus it could embed json object
	Emitter   string              `cql:"emitter"      json:"emitter,omitempty"`
	NotifId   string              `cql:"notif_id"     json:"notif_id,omitempty"`
	Reference string              `cql:"reference"    json:"reference,omitempty"`
	Type      string              `cql:"type"         json:"type,omitempty"`
	UserId    string              `cql:"user_id"      json:"user_id"          frontend:"omit"`
	Children  []NotificationModel `cql:"-"            json:"-"`
}

// model to retrieve default durations for TTLcodes
type NotificationTTL struct {
	// PRIMARY KEY (ttl_code)
	TTLcode     string `cql:"ttl_code"      json:"ttl_code"`
	TTLduration int    `cql:"ttl_duration"  json:"ttl_duration"`
	Description string `cql:"description"   json:"description"`
}

const (
	// types list for Notification.Type property
	EventNotif    = "event"
	InfoNotif     = "info"
	FeedbackNotif = "feedback"
	WarningNotif  = "warning"
	TeaserNotif   = "teaser"
	ErrorNotif    = "error"
	AlertNotif    = "alert"
	BatchNotif    = "batch"

	// TTL codes stored in notification_ttl table
	ShortLived = "short-lived"
	MidLived   = "mid-lived"
	LongLived  = "long-lived"
	ShortTerm  = "short-term"
	MidTerm    = "mid-term"
	LongTerm   = "long-term"
	Forever    = "forever"
)

// return a JSON representation of Notification suitable for frontend client
func (n *Notification) MarshalFrontEnd() ([]byte, error) {
	sibling := NotificationModel{
		Body:      n.Body,
		Emitter:   n.Emitter,
		NotifId:   n.NotifId.String(),
		Reference: n.Reference,
		Type:      n.Type,
		UserId:    n.User.UserId.String(),
	}
	if len(n.Children) > 0 {
		sibling.Children = make([]NotificationModel, len(n.Children))
		for i, child := range n.Children {
			c := NotificationModel{
				Body:      child.Body,
				Emitter:   child.Emitter,
				NotifId:   child.NotifId.String(),
				Reference: child.Reference,
				Type:      child.Type,
				UserId:    child.User.UserId.String(),
			}
			sibling.Children[i] = c
		}
	}
	return JSONMarshaller("frontend", &sibling)
}

// UnmarshalCQLMap hydrates a Notification with data from a map[string]interface{}
// typical usage is for unmarshaling response from Cassandra backend
func (n *Notification) UnmarshalCQLMap(input map[string]interface{}) {

	if userId, ok := input["user_id"].(gocql.UUID); ok {
		n.User = &User{}
		n.User.UserId.UnmarshalBinary(userId.Bytes())
	}

	if id, ok := input["notif_id"].(gocql.UUID); ok {
		n.NotifId.UnmarshalBinary(id.Bytes())
	}

	if body, ok := input["body"].([]byte); ok {
		n.Body = string(body)
	}
	// body should be a json document
	// it may embed children notifications that need to be extracted to the children property
	if gjson.Valid(n.Body) {
		if children := gjson.Get(n.Body, "children"); children.IsArray() {
			n.Children = []Notification{}
			children.ForEach(func(key, value gjson.Result) bool {
				if value.IsObject() {
					var child NotificationModel
					if err := gjson.Unmarshal([]byte(value.Raw), &child); err == nil {
						n.Children = append(n.Children, Notification{
							Body:      child.Body,
							Emitter:   child.Emitter,
							NotifId:   UUID(uuid.FromStringOrNil(child.NotifId)),
							Reference: child.Reference,
							Type:      child.Type,
							User:      &User{UserId: UUID(uuid.FromStringOrNil(child.UserId))},
						})
					}
				}
				return true
			})
		}
		n.ChildrenCount = int(gjson.Get(n.Body, "children_count").Int())
	}

	if emitter, ok := input["emitter"].(string); ok {
		n.Emitter = emitter
	}

	if reference, ok := input["reference"].(string); ok {
		n.Reference = reference
	}

	if typ, ok := input["type"].(string); ok {
		n.Type = typ
	}
}

// MarshalJSON implements json.Marshaler interface
// using a bespoke implementation allows to embed notification's body as a json object instead of a string
func (n *NotificationModel) MarshalJSON() ([]byte, error) {
	return JSONMarshaller("frontend", n)
}
