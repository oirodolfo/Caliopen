// Copyleft (ɔ) 2018 The Caliopen contributors.
// Use of this source code is governed by a GNU AFFERO GENERAL PUBLIC
// license (AGPL) that can be found in the LICENSE file.

package objects

import (
	"bytes"
	"encoding/json"
	log "github.com/Sirupsen/logrus"
	"github.com/gocql/gocql"
	"github.com/satori/go.uuid"
	"regexp"
	"strings"
	"time"
)

type (
	// object stored in db with primary keys on user_id and identity_id
	UserIdentity struct {
		Credentials *Credentials      `cql:"credentials"        json:"credentials,omitempty"                            patch:"user"`
		DisplayName string            `cql:"display_name"       json:"display_name"                                     patch:"user"`
		Id          UUID              `cql:"identity_id"        json:"identity_id"`
		Identifier  string            `cql:"identifier"         json:"identifier"                                                     ` // for example: me@caliopen.org, @mastodon_account
		Infos       map[string]string `cql:"infos"              json:"infos"                                            patch:"user"  `
		LastCheck   time.Time         `cql:"last_check"         json:"last_check,omitempty"                 formatter:"RFC3339Milli"`
		Protocol    string            `cql:"protocol"           json:"protocol"                                                       ` // for example: smtp, imap, mastodon
		Status      string            `cql:"status"             json:"status"                                           patch:"user"`   // for example : active, inactive, deleted
		Type        string            `cql:"type"               json:"type"                                                           ` // for example : local, remote
		UserId      UUID              `cql:"user_id"            json:"user_id"              frontend:"omit"`
	}

	// cassandra tables to lookup identities by identifier, protocol, user_id and/or type
	// lookup_tables :  identity_lookup(identifier + protocol + user_id)
	//                  identity_type_lookup(type + user_id)
	IdentityLookup struct {
		Identifier string `cql:"identifier"`
		Protocol   string `cql:"protocol"`
		UserId     UUID   `cql:"user_id"`
		IdentityId UUID   `cql:"identity_id"`
	}
	IdentityTypeLookup struct {
		Type       string `cql:"type"`
		UserId     UUID   `cql:"user_id"`
		IdentityId UUID   `cql:"identity_id"`
	}
)

func (ui *UserIdentity) NewEmpty() interface{} {
	nui := new(UserIdentity)
	nui.Infos = map[string]string{}
	return nui
}

func (ui *UserIdentity) UnmarshalJSON(b []byte) error {
	input := map[string]interface{}{}
	if err := json.Unmarshal(b, &input); err != nil {
		return err
	}

	return ui.UnmarshalMap(input)
}

func (ui *UserIdentity) UnmarshalMap(input map[string]interface{}) error {
	if credentials, ok := input["credentials"]; ok && credentials != nil {
		cred := &Credentials{}
		cred.UnmarshalMap(credentials.(map[string]interface{}))
		ui.Credentials = cred
	}
	if dn, ok := input["display_name"].(string); ok {
		ui.DisplayName = dn
	}
	if id, ok := input["identity_id"].(string); ok {
		if id, err := uuid.FromString(id); err == nil {
			ui.Id.UnmarshalBinary(id.Bytes())
		}
	}
	if identifier, ok := input["identifier"].(string); ok {
		ui.Identifier = identifier
	}
	if infos, ok := input["infos"].(map[string]interface{}); ok {
		ui.Infos = make(map[string]string)
		for k, v := range infos {
			ui.Infos[k] = v.(string)
		}
	}

	if lc, ok := input["last_check"]; ok {
		ui.LastCheck, _ = time.Parse(time.RFC3339Nano, lc.(string))
	}
	if protocol, ok := input["protocol"].(string); ok {
		ui.Protocol = protocol
	}
	if status, ok := input["status"].(string); ok {
		ui.Status = status
	}
	if t, ok := input["type"].(string); ok {
		ui.Type = t
	}
	if userid, ok := input["user_id"].(string); ok {
		if id, err := uuid.FromString(userid); err == nil {
			ui.UserId.UnmarshalBinary(id.Bytes())
		}
	}
	return nil
}

func (ui *UserIdentity) JsonTags() (tags map[string]string) {
	return jsonTags(ui)
}

func (ui *UserIdentity) SortSlices() {
	//no slice to sort
}

// ensure mandatory properties are set, also default values.
func (ui *UserIdentity) MarshallNew(args ...interface{}) {
	if len(ui.Id) == 0 || (bytes.Equal(ui.Id.Bytes(), EmptyUUID.Bytes())) {
		ui.Id.UnmarshalBinary(uuid.NewV4().Bytes())
	}
	if (len(ui.UserId) == 0 || bytes.Equal(ui.UserId.Bytes(), EmptyUUID.Bytes())) && len(args) == 1 {
		switch args[0].(type) {
		case UUID:
			ui.UserId = args[0].(UUID)
		}
	}

}

// SetDefaults fills UserIdentity with default keys and values according to the type of the remote identity
func (ui *UserIdentity) SetDefaults() {
	defaults := map[string]string{}

	switch ui.Protocol {
	case EmailProtocol:
		defaults = map[string]string{
			"lastseenuid":  "",
			"lastsync":     "",   // RFC3339 date string
			"inserver":     "",   // server hostname[|port]
			"outserver":    "",   // server hostname[|port]
			"uidvalidity":  "",   // uidvalidity to invalidate data if needed (see RFC4549#section-4.1)
			"pollinterval": "15", // how often remote account should be polled, in minutes.
		}
	case TwitterProtocol, MastodonProtocol:
		defaults = map[string]string{
			"lastseendm":   "",
			"lastsync":     "",  // RFC3339 date string
			"pollinterval": "2", // how often remote account should be polled, in minutes.
		}
	}

	if ui.Infos == nil {
		(*ui).Infos = defaults
	} else {
		for default_key, default_value := range defaults {
			if v, ok := ui.Infos[default_key]; !ok || v == "" {
				(*ui).Infos[default_key] = default_value
			}
		}
	}

	// try to infer authentication mechanism
	if authType, ok := ui.Infos["authtype"]; !ok || authType == "" {
		loginPassRegex, _ := regexp.Compile(`(?i)password|login`)
		oauth1Regex, _ := regexp.Compile(`(?i)auth1`)
		oauth2Regex, _ := regexp.Compile(`(?i)auth2`)
	credentialsLoop:
		for k, _ := range *ui.Credentials {
			switch {
			case loginPassRegex.MatchString(k):
				(*ui).Infos["authtype"] = LoginPassword
				break credentialsLoop
			case oauth1Regex.MatchString(k):
				(*ui).Infos["authtype"] = Oauth1
				break credentialsLoop
			case oauth2Regex.MatchString(k):
				(*ui).Infos["authtype"] = Oauth2
				break credentialsLoop
			}
		}
	}

	if ui.Status == "" {
		(*ui).Status = "active"
	}

	if ui.Type == "" {
		(*ui).Type = "remote"
	}
	(*ui).LastCheck = time.Time{}
	// try to set DisplayName and Identifier if it is missing
	if ui.Identifier == "" {
		switch ui.Protocol {
		case ImapProtocol:
			(*ui).Identifier, _ = (*ui.Credentials)["username"]
		}
	}
	if ui.DisplayName == "" {
		ui.DisplayName = ui.Identifier
	}
	ui.Identifier = strings.ToLower(ui.Identifier)
}

func (ui *UserIdentity) UnmarshalCQLMap(input map[string]interface{}) error {
	if credentials, ok := input["credentials"]; ok && credentials != nil {
		cred := &Credentials{}
		cred.UnmarshalCQLMap(credentials.(map[string]string))
		ui.Credentials = cred
	}
	if dn, ok := input["display_name"].(string); ok {
		ui.DisplayName = dn
	}
	if id, ok := input["identity_id"].(gocql.UUID); ok {
		ui.Id.UnmarshalBinary(id.Bytes())
	}
	if identifier, ok := input["identifier"].(string); ok {
		ui.Identifier = identifier
	}
	if infos, ok := input["infos"].(map[string]string); ok {
		ui.Infos = make(map[string]string)
		for k, v := range infos {
			ui.Infos[k] = v
		}
	}
	if lc, ok := input["last_check"].(time.Time); ok {
		ui.LastCheck = lc
	}
	if protocol, ok := input["protocol"].(string); ok {
		ui.Protocol = protocol
	}
	if status, ok := input["status"].(string); ok {
		ui.Status = status
	}
	if t, ok := input["type"].(string); ok {
		ui.Type = t
	}
	if userid, ok := input["user_id"].(gocql.UUID); ok {
		ui.UserId.UnmarshalBinary(userid.Bytes())
	}
	return nil
}

func (ui *UserIdentity) MarshalFrontEnd() ([]byte, error) {
	return JSONMarshaller("frontend", ui)
}

/**`HasLookup` interface implementation for UserIdentity
	to ensure lookup tables consistency
 **/
func (userIdentity *UserIdentity) GetLookupsTables() map[string]StoreLookup {
	return map[string]StoreLookup{
		"identity_lookup":      &IdentityLookup{},
		"identity_type_lookup": &IdentityTypeLookup{},
	}
}

// GetLookupKeys returns a chan to iterate over fields and values that make up the lookup tables keys
func (userIdentity *UserIdentity) GetLookupKeys() <-chan StoreLookup {
	getter := make(chan StoreLookup)

	go func(chan StoreLookup) {
		getter <- &IdentityLookup{
			Identifier: userIdentity.Identifier,
			Protocol:   userIdentity.Protocol,
			UserId:     userIdentity.UserId,
		}
		getter <- &IdentityTypeLookup{
			Type:   userIdentity.Type,
			UserId: userIdentity.UserId,
		}
		close(getter)
	}(getter)

	return getter
}

// CleanupLookups implements StoreLookup interface.
// It returns a func which removes IdentityLookup related to the UserIdentity given as param of the variadic func.
func (il *IdentityLookup) CleanupLookups(identities ...interface{}) func(session *gocql.Session) error {
	if len(identities) == 1 {
		identity := identities[0].(*UserIdentity)
		return func(session *gocql.Session) error {
			err := session.Query(`DELETE FROM identity_lookup WHERE identifier = ? AND protocol = ? AND user_id = ?`,
				identity.Identifier, identity.Protocol, identity.UserId.String()).Exec()
			if err != nil {
				return err
			}

			return nil
		}
	}
	return nil
}

// UpdateLookups iterates over remote identity's lookups to add/update them to the relevant table,
// then it deletes lookups references that are no more linked to an embedded key which has been removed,
// `identites` param should have one item in the context of a creation or 2 items [new, old] in the context of an update
func (il *IdentityLookup) UpdateLookups(identities ...interface{}) func(session *gocql.Session) error {
	identitiesLen := len(identities)
	update := false
	if identitiesLen > 0 {
		newIdentity := identities[0].(*UserIdentity)
		var oldLookup map[string]*IdentityLookup
		return func(session *gocql.Session) error {
			if identitiesLen == 2 && identities[1] != nil {
				// it's an update
				update = true
				oldIdentity := identities[1].(*UserIdentity)
				oldLookup = map[string]*IdentityLookup{} // build strings with cassa's keys
				for lookup := range oldIdentity.GetLookupKeys() {
					lkp := lookup.(*IdentityLookup)
					oldLookup[lkp.Identifier+lkp.Protocol+lkp.UserId.String()] = lkp
				}
			}
			lkp := &IdentityLookup{
				Identifier: newIdentity.Identifier,
				Protocol:   newIdentity.Protocol,
				UserId:     newIdentity.UserId,
			}
			// try to get identity_id
			var identityId gocql.UUID
			session.Query(`SELECT identity_id FROM identity_lookup WHERE identifier = ? AND protocol = ? AND user_id = ?`,
				lkp.Identifier,
				lkp.Protocol,
				lkp.UserId).Scan(&identityId)
			if identityId.String() == "" ||
				identityId.String() != newIdentity.UserId.String() { // identity_id not found or changed => set one
				err := session.Query(`INSERT INTO identity_lookup (identifier, protocol, user_id, identity_id) VALUES (?,?,?,?)`,
					lkp.Identifier,
					lkp.Protocol,
					lkp.UserId,
					newIdentity.Id.String()).Exec()
				if err != nil {
					log.WithError(err).Warnf(`[CassandraBackend] UpdateLookups INSERT failed for user: %s, identifier: %s`,
						lkp.UserId,
						lkp.Identifier)
				}
			}
			if update {
				// remove keys in current states,
				// thus oldLookup map will only holds remaining entries that are not in the new state
				delete(oldLookup, lkp.Identifier+lkp.Protocol+lkp.UserId.String())
			}
			if len(oldLookup) > 0 {
				// it remains lookups in the map, meaning identifier has been changed
				// need to remove it from lookup table
				for _, lookup := range oldLookup {
					err := session.Query(`DELETE FROM identity_lookup WHERE identifier = ? AND protocol = ? AND user_id = ?`,
						lookup.Identifier,
						lookup.Protocol,
						lookup.UserId).Exec()
					if err != nil {
						return err
					}
				}
			}

			return nil
		}
	}
	return nil
}

// CleanupLookups implements StoreLookup interface.
// It returns a func which removes IdentityTypeLookup related to the UserIdentity given as param of the variadic func.
func (itl *IdentityTypeLookup) CleanupLookups(identities ...interface{}) func(session *gocql.Session) error {
	if len(identities) == 1 {
		identity := identities[0].(*UserIdentity)
		return func(session *gocql.Session) error {
			err := session.Query(`DELETE FROM identity_type_lookup WHERE type = ? AND user_id = ? AND identity_id = ?`,
				identity.Type, identity.UserId.String(), identity.Id.String()).Exec()
			if err != nil {
				return err
			}

			return nil
		}
	}
	return nil
}

func (itl *IdentityTypeLookup) UpdateLookups(identities ...interface{}) func(session *gocql.Session) error {
	identitiesLen := len(identities)
	update := false
	if identitiesLen > 0 {
		newIdentity := identities[0].(*UserIdentity)
		var oldLookup map[string]*IdentityTypeLookup
		return func(session *gocql.Session) error {
			if identitiesLen == 2 && identities[1] != nil {
				// it's an update
				update = true
				oldIdentity := identities[1].(*UserIdentity)
				oldLookup = map[string]*IdentityTypeLookup{} // build strings with cassa's keys
				for lookup := range oldIdentity.GetLookupKeys() {
					lkp := lookup.(*IdentityTypeLookup)
					oldLookup[lkp.Type+lkp.UserId.String()+lkp.IdentityId.String()] = lkp
				}
			}
			lkp := &IdentityTypeLookup{
				Type:   newIdentity.Type,
				UserId: newIdentity.UserId,
			}
			err := session.Query(`INSERT INTO identity_type_lookup (type, user_id, identity_id) VALUES (?,?,?)`,
				lkp.Type,
				lkp.UserId,
				newIdentity.Id.String()).Exec()
			if err != nil {
				log.WithError(err).Warnf(`[CassandraBackend] UpdateLookups INSERT failed for user: %s, type: %s`,
					lkp.UserId,
					lkp.Type)
			}
			if update {
				// remove keys in current states,
				// thus oldLookup map will only holds remaining entries that are not in the new state
				delete(oldLookup, lkp.Type+lkp.UserId.String()+lkp.IdentityId.String())
			}
			if len(oldLookup) > 0 {
				// it remains lookups in the map, meaning identifier has been changed
				// need to remove it from lookup table
				for _, lookup := range oldLookup {
					err := session.Query(`DELETE FROM identity_type_lookup WHERE type = ? AND user_id = ? AND identity_id = ?`,
						lookup.Type,
						lookup.UserId,
						lookup.IdentityId).Exec()
					if err != nil {
						return err
					}
				}
			}

			return nil
		}
	}
	return nil
}

type ByUUID []UUID

func (p ByUUID) Len() int {
	return len(p)
}

func (p ByUUID) Less(i, j int) bool {
	return p[i].String() < p[j].String()
}

func (p ByUUID) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}
