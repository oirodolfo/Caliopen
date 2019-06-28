// Copyleft (ɔ) 2017 The Caliopen contributors.
// Use of this source code is governed by a GNU AFFERO GENERAL PUBLIC
// license (AGPL) that can be found in the LICENSE file.

package backends

import (
	. "github.com/CaliOpen/Caliopen/src/backend/defs/go-objects"
	"github.com/gocql/gocql"
)

type APIStorage interface {
	AttachmentStorage
	CredentialsStorage
	ContactStorage
	DevicesStorage
	DiscussionStorage
	IdentityStorage
	KeysStorage
	MessageStorage
	UrisStorage
	TagsStorage
	UserNameStorage
	UserStorage
	ProviderStorage
	GetSession() *gocql.Session
}

type APIIndex interface {
	MessageIndex
	ContactIndex
	DiscussionIndex
	RecipientsSuggest(user *UserInfo, query_string string) (suggests []RecipientSuggestion, err error)
	Search(search IndexSearch) (result *IndexResult, err error)
}
