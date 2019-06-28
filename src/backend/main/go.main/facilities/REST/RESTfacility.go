// Copyleft (ɔ) 2017 The Caliopen contributors.
// Use of this source code is governed by a GNU AFFERO GENERAL PUBLIC
// license (AGPL) that can be found in the LICENSE file.

package REST

import (
	. "github.com/CaliOpen/Caliopen/src/backend/defs/go-objects"
	"github.com/CaliOpen/Caliopen/src/backend/main/go.backends"
	"github.com/CaliOpen/Caliopen/src/backend/main/go.backends/cache"
	"github.com/CaliOpen/Caliopen/src/backend/main/go.backends/index/elasticsearch"
	"github.com/CaliOpen/Caliopen/src/backend/main/go.backends/store/cassandra"
	"github.com/CaliOpen/Caliopen/src/backend/main/go.main/facilities/Notifications"
	log "github.com/Sirupsen/logrus"
	"github.com/gocql/gocql"
	"github.com/nats-io/go-nats"
	"github.com/tidwall/gjson"
	"io"
)

type (
	RESTservices interface {
		UsernameIsAvailable(string) (bool, error)
		SuggestRecipients(user *UserInfo, query_string string) (suggests []RecipientSuggestion, err error)
		GetSettings(user_id string) (settings *Settings, err error)
		//contacts
		CreateContact(user *UserInfo, contact *Contact) error
		RetrieveContacts(filter IndexSearch) (contacts []*Contact, totalFound int64, err error)
		RetrieveContact(userID, contactID string) (*Contact, error)
		RetrieveUserContact(userID string) (*Contact, error)
		UpdateContact(user *UserInfo, contact, oldContact *Contact, update map[string]interface{}) error
		PatchContact(user *UserInfo, patch []byte, contactID string) error
		DeleteContact(user *UserInfo, contactID string) error
		ContactExists(userID, contactID string) bool
		LookupContactByUri(userID, uri string) ([]*Contact, int64, error)
		ImportVcardFile(info *UserInfo, file io.Reader) error
		//identities
		RetrieveContactIdentities(user_id, contact_id string) (identities []ContactIdentity, err error)
		RetrieveLocalIdentities(user_id string) (identities []UserIdentity, err error)
		CreateUserIdentity(identity *UserIdentity) CaliopenError
		RetrieveRemoteIdentities(userId string, withCredentials bool) (ids []*UserIdentity, err CaliopenError)
		RetrieveUserIdentity(userId, RemoteId string, withCredentials bool) (id *UserIdentity, err CaliopenError)
		UpdateUserIdentity(identity, oldIdentity *UserIdentity, update map[string]interface{}) CaliopenError
		PatchUserIdentity(patch []byte, userId, RemoteId string) CaliopenError
		DeleteUserIdentity(userId, remoteId string) CaliopenError
		IsRemoteIdentity(userId, remoteId string) bool
		//providers
		RetrieveProvidersList() (providers []Provider, err error)
		GetProviderOauthFor(userID, provider, instance string) (Provider, CaliopenError)
		CreateTwitterIdentity(requestToken, verifier string) (remoteId string, err CaliopenError)
		CreateGmailIdentity(state, code string) (remoteId string, err CaliopenError)
		CreateMastodonIdentity(state, code string) (remoteId string, err CaliopenError)
		//discussions
		GetDiscussionsList(user *UserInfo, ILrange, PIrange [2]int8, limit, offset int) ([]Discussion, int, error)
		DiscussionMetadata(user *UserInfo, discussionId string) (Discussion, error)
		//messages
		GetMessagesList(filter IndexSearch) (messages []*Message, totalFound int64, err error)
		GetMessagesRange(filter IndexSearch) (messages []*Message, totalFound int64, err error)
		GetMessage(user *UserInfo, message_id string) (message *Message, err error)
		SendDraft(user *UserInfo, msg_id string) (msg *Message, err error)
		SetMessageUnread(user *UserInfo, message_id string, status bool) error
		GetRawMessage(raw_message_id string) (message []byte, err error)
		//attachments
		AddAttachment(user *UserInfo, message_id, filename, content_type string, file io.Reader) (attachmentURL string, err error)
		DeleteAttachment(user *UserInfo, message_id string, attchmt_id string) CaliopenError
		OpenAttachment(user_id, message_id string, attchmtIndex string) (meta map[string]string, content io.Reader, err error)
		//tags
		RetrieveUserTags(user_id string) (tags []Tag, err CaliopenError)
		CreateTag(tag *Tag) CaliopenError
		RetrieveTag(user_id, tag_id string) (tag Tag, err CaliopenError)
		UpdateTag(tag *Tag) CaliopenError
		PatchTag(patch []byte, user_id, tag_name string) CaliopenError
		DeleteTag(user_id, tag_name string) CaliopenError
		UpdateResourceTags(user *UserInfo, resourceID, resourceType string, patch []byte) CaliopenError
		//search
		Search(IndexSearch) (result *IndexResult, err error)
		//users
		PatchUser(user_id string, patch *gjson.Result, notifier Notifications.Notifiers) error
		RequestPasswordReset(payload PasswordResetRequest, notifier Notifications.Notifiers) error
		ValidatePasswordResetToken(token string) (session *TokenSession, err error)
		ResetUserPassword(token, new_password string, notifier Notifications.Notifiers) error
		DeleteUser(payload ActionsPayload) CaliopenError
		//devices
		CreateDevice(device *Device) CaliopenError
		RetrieveDevices(userId string) ([]Device, CaliopenError)
		RetrieveDevice(userId, deviceId string) (*Device, CaliopenError)
		UpdateDevice(device, oldDevice *Device, update map[string]interface{}) CaliopenError
		PatchDevice(patch []byte, userId, deviceId string) CaliopenError
		DeleteDevice(userId, deviceId string) CaliopenError
		RequestDeviceValidation(userId, deviceId, channel string, notifier Notifications.Notifiers) CaliopenError
		ConfirmDeviceValidation(userId, token string) CaliopenError
		//keys
		CreatePGPPubKey(label string, pubkey []byte, contact *Contact) (*PublicKey, CaliopenError)
		RetrieveContactPubKeys(userId, contactId string) (pubkeys PublicKeys, err CaliopenError)
		RetrievePubKey(userId, resourceId, keyId string) (pubkey *PublicKey, err CaliopenError)
		DeletePubKey(pubkey *PublicKey) CaliopenError
		PatchPubKey(patch []byte, userId, resourceId, keyId string) CaliopenError
	}
	RESTfacility struct {
		Cache      backends.APICache
		index      backends.APIIndex
		natsTopics map[string]string
		nats_conn  *nats.Conn
		providers  map[string]Provider
		store      backends.APIStorage
		Hostname   string
	}
)

func NewRESTfacility(config CaliopenConfig, nats_conn *nats.Conn) (rest_facility *RESTfacility) {
	rest_facility = new(RESTfacility)
	rest_facility.nats_conn = nats_conn
	rest_facility.natsTopics = map[string]string{
		Nats_outSMTP_topicKey:    config.NatsConfig.OutSMTP_topic,
		Nats_outIMAP_topicKey:    config.NatsConfig.OutIMAP_topic,
		Nats_Contacts_topicKey:   config.NatsConfig.Contacts_topic,
		Nats_outTwitter_topicKey: config.NatsConfig.OutTWITTER_topic,
		Nats_Keys_topicKey:       config.NatsConfig.Keys_topic,
		Nats_IdPoller_topicKey:   config.NatsConfig.IdPoller_topic,
	}
	switch config.RESTstoreConfig.BackendName {
	case "cassandra":
		cassaConfig := store.CassandraConfig{
			Hosts:       config.RESTstoreConfig.Hosts,
			Keyspace:    config.RESTstoreConfig.Keyspace,
			Consistency: gocql.Consistency(config.RESTstoreConfig.Consistency),
			UseVault:    config.RESTstoreConfig.UseVault,
		}
		if config.RESTstoreConfig.ObjStoreType == "s3" {
			cassaConfig.WithObjStore = true
			cassaConfig.OSSConfig.Endpoint = config.RESTstoreConfig.OSSConfig.Endpoint
			cassaConfig.OSSConfig.AccessKey = config.RESTstoreConfig.OSSConfig.AccessKey
			cassaConfig.OSSConfig.SecretKey = config.RESTstoreConfig.OSSConfig.SecretKey
			cassaConfig.OSSConfig.Location = config.RESTstoreConfig.OSSConfig.Location
			cassaConfig.OSSConfig.RawMsgBucket = config.RESTstoreConfig.OSSConfig.Buckets["raw_messages"]
			cassaConfig.OSSConfig.AttachmentBucket = config.RESTstoreConfig.OSSConfig.Buckets["temporary_attachments"]
		}
		if config.RESTstoreConfig.UseVault {
			cassaConfig.HVaultConfig.Url = config.RESTstoreConfig.VaultConfig.Url
			cassaConfig.HVaultConfig.Username = config.RESTstoreConfig.VaultConfig.Username
			cassaConfig.HVaultConfig.Password = config.RESTstoreConfig.VaultConfig.Password
		}
		backend, err := store.InitializeCassandraBackend(cassaConfig)
		if err != nil {
			log.WithError(err).Fatalf("initalization of %s backend failed", config.RESTstoreConfig.BackendName)
		}

		rest_facility.store = backends.APIStorage(backend) // type conversion

	default:
		log.Fatalf("unknown backend: %s", config.RESTstoreConfig.BackendName)
	}

	switch config.RESTindexConfig.IndexName {
	case "elasticsearch":
		esConfig := index.ElasticSearchConfig{
			Urls: config.RESTindexConfig.Hosts,
		}
		indx, err := index.InitializeElasticSearchIndex(esConfig)
		if err != nil {
			log.WithError(err).Fatalf("initalization of %s index failed", config.RESTindexConfig.IndexName)
		}
		rest_facility.index = backends.APIIndex(indx) // type conversion
	default:
		log.Fatalf("unknown index: %s", config.RESTindexConfig.IndexName)
	}
	var err error
	rest_facility.Cache, err = cache.InitializeRedisBackend(config.CacheConfig)
	if err != nil {
		log.WithError(err).Fatal("initialization of Redis cache failed")
	}

	rest_facility.providers = map[string]Provider{}
	for _, provider := range config.Providers {
		if provider.Name != "" {
			rest_facility.providers[provider.Name] = provider
		}
	}

	rest_facility.Hostname = config.Hostname
	return rest_facility
}
