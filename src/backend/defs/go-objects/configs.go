// Copyleft (ɔ) 2017 The Caliopen contributors.
// Use of this source code is governed by a GNU AFFERO GENERAL PUBLIC
// license (AGPL) that can be found in the LICENSE file.

package objects

type (
	CaliopenConfig struct {
		CacheConfig     CacheConfig
		Hostname        string
		NatsConfig      NatsConfig
		NotifierConfig  NotifierConfig
		Providers       []Provider `mapstructure:"Providers"`
		RESTindexConfig RESTIndexConfig
		RESTstoreConfig RESTstoreConfig
	}

	// REST API
	RESTstoreConfig struct {
		BackendName  string   `mapstructure:"backend_name"`
		Consistency  uint16   `mapstructure:"consistency_level"`
		Hosts        []string `mapstructure:"hosts"`
		Keyspace     string   `mapstructure:"keyspace"`
		OSSConfig    `mapstructure:"object_store_settings"`
		ObjStoreType string `mapstructure:"object_store"`
		SizeLimit    uint64 `mapstructure:"raw_size_limit"` // max size for db (in bytes)
		UseVault     bool   `mapstructure:"use_vault"`
		VaultConfig  `mapstructure:"vault_settings"`
	}

	RESTIndexConfig struct {
		IndexName string   `mapstructure:"index_name"`
		Hosts     []string `mapstructure:"hosts"`
	}

	// redis
	CacheConfig struct {
		Host     string `mapstructure:"host"`
		Password string `mapstructure:"password"`
		Db       int    `mapstructure:"db"`
	}

	// NATS
	NatsConfig struct {
		Url               string `mapstructure:"url"`
		NatsQueue         string `mapstructure:"nats_queue"`
		OutSMTP_topic     string `mapstructure:"outSMTP_topic"`
		OutIMAP_topic     string `mapstructure:"outIMAP_topic"`
		OutTWITTER_topic  string `mapstructure:"outTWITTER_topic"`
		OutMASTODON_topic string `mapstructure:"outMASTODON_topic"`
		Contacts_topic    string `mapstructure:"contacts_topic"`
		Keys_topic        string `mapstructure:"keys_topic"`
		Users_topic       string `mapstructure:"users_topic"`
		IdPoller_topic    string `mapstructure:"idpoller_topic"`
	}
	// Cassandra
	StoreConfig struct {
		Hosts       []string    `mapstructure:"hosts"`
		Keyspace    string      `mapstructure:"keyspace"`
		Consistency uint16      `mapstructure:"consistency_level"`
		SizeLimit   uint64      `mapstructure:"raw_size_limit"` // max size to store (in bytes)
		ObjectStore string      `mapstructure:"object_store"`
		OSSConfig   OSSConfig   `mapstructure:"object_store_settings"`
		UseVault    bool        `mapstructure:"use_vault"`
		VaultConfig VaultConfig `mapstructure:"vault_settings"`
	}

	// Elasticsearch
	IndexConfig struct {
		Urls []string `mapstructure:"urls"`
	}

	// Objects Store
	OSSConfig struct {
		Endpoint  string            `mapstructure:"endpoint"`
		AccessKey string            `mapstructure:"access_key"`
		SecretKey string            `mapstructure:"secret_key"`
		Location  string            `mapstructure:"location"`
		Buckets   map[string]string `mapstructure:"buckets"`
	}

	// Notifications facility
	NotifierConfig struct {
		AdminUsername string `mapstructure:"admin_username"`
		BaseUrl       string `mapstructure:"base_url"`       // url upon which to build custom links sent to users. No trailing slash please.
		TemplatesPath string `mapstructure:"templates_path"` // path to templates Notifiers may need to access to
	}

	// Hashicorp Vault
	VaultConfig struct {
		Url      string `mapstructure:"url"`
		Username string `mapstructure:"username"`
		Password string `mapstructure:"password"`
	}

	// providers
	ProvidersConfig struct {
		Providers []Provider `mapstructure:"Providers"`
	}

	LDAConfig struct {
		AppVersion       string         `mapstructure:"version"`
		BrokerType       string         `mapstructure:"broker_type"`
		ContactsTopic    string         `mapstructure:"contacts_topic"`
		InTopic          string         `mapstructure:"in_topic"`
		InWorkers        int            `mapstructure:"lda_workers_size"`
		IndexName        string         `mapstructure:"index_name"`
		LogReceivedMails bool           `mapstructure:"log_received_mails"`
		NatsListeners    int            `mapstructure:"nats_listeners"`
		NatsQueue        string         `mapstructure:"nats_queue"`
		NatsURL          string         `mapstructure:"nats_url"`
		OutTopic         string         `mapstructure:"out_topic"`
		PrimaryMailHost  string         `mapstructure:"primary_mail_host"`
		StoreName        string         `mapstructure:"store_name"`
		IndexConfig      IndexConfig    `mapstructure:"index_settings"`
		NotifierConfig   NotifierConfig `mapstructure:"NotifierConfig"`
		Providers        []Provider     `mapstructure:"Providers"`
		StoreConfig      StoreConfig    `mapstructure:"store_settings"`
	}
)
