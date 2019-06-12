// Copyleft (ɔ) 2017 The Caliopen contributors.
// Use of this source code is governed by a GNU AFFERO GENERAL PUBLIC
// license (AGPL) that can be found in the LICENSE file.

// package email_broker handles codec/decodec between external emails and Caliopen message format
package email_broker

import (
	"errors"
	. "github.com/CaliOpen/Caliopen/src/backend/defs/go-objects"
	"github.com/CaliOpen/Caliopen/src/backend/main/go.backends"
	"github.com/CaliOpen/Caliopen/src/backend/main/go.backends/index/elasticsearch"
	"github.com/CaliOpen/Caliopen/src/backend/main/go.backends/store/cassandra"
	"github.com/CaliOpen/Caliopen/src/backend/main/go.main/facilities/Notifications"
	log "github.com/Sirupsen/logrus"
	"github.com/gocql/gocql"
	"github.com/nats-io/go-nats"
	"math/rand"
	"time"
)

type (
	EmailBroker struct {
		Config            LDAConfig
		Connectors        EmailBrokerConnectors
		Index             backends.LDAIndex
		NatsConn          *nats.Conn
		Notifier          Notifications.Notifiers
		Store             backends.LDAStore
		natsSubscriptions []*nats.Subscription
	}

	EmailBrokerConnectors struct {
		Egress  chan *SmtpEmail
		Ingress chan *SmtpEmail
	}

	SmtpEmail struct {
		EmailMessage *EmailMessage
		MTAparams    *MTAparams
		Response     chan *EmailDeliveryAck
		Batch        *Notifications.BatchNotification
	}

	natsOrder struct {
		MessageId string `json:"message_id"`
		Order     string `json:"order"`
		UserId    string `json:"user_id"`
	}

	//MTAparams is for embedding credentials to deliver email via a remote SMTP server
	MTAparams struct {
		AuthType string
		Host     string
		Password string
		User     string
	}

	// DeliveryAck holds reply from nats when using request/reply system for email
	EmailDeliveryAck struct {
		EmailMessage *EmailMessage `json:"-"`
		Err          bool          `json:"error"`
		Response     string        `json:"message,omitempty"`
	}
)

var (
	broker *EmailBroker
)

func Initialize(conf LDAConfig) (broker *EmailBroker, connectors EmailBrokerConnectors, err error) {
	var e error

	rand.Seed(time.Now().UnixNano())

	broker = &EmailBroker{}
	broker.Config = conf
	switch conf.StoreName {
	case "cassandra":
		c := store.CassandraConfig{
			Hosts:       conf.StoreConfig.Hosts,
			Keyspace:    conf.StoreConfig.Keyspace,
			Consistency: gocql.Consistency(conf.StoreConfig.Consistency),
			SizeLimit:   conf.StoreConfig.SizeLimit,
			UseVault:    conf.StoreConfig.UseVault,
		}
		if conf.StoreConfig.ObjectStore == "s3" {
			c.WithObjStore = true
			c.Endpoint = conf.StoreConfig.OSSConfig.Endpoint
			c.AccessKey = conf.StoreConfig.OSSConfig.AccessKey
			c.SecretKey = conf.StoreConfig.OSSConfig.SecretKey
			c.RawMsgBucket = conf.StoreConfig.OSSConfig.Buckets["raw_messages"]
			c.AttachmentBucket = conf.StoreConfig.OSSConfig.Buckets["temporary_attachments"]
			c.Location = conf.StoreConfig.OSSConfig.Location
		}
		if conf.StoreConfig.UseVault {
			c.HVaultConfig.Url = conf.StoreConfig.VaultConfig.Url
			c.HVaultConfig.Username = conf.StoreConfig.VaultConfig.Username
			c.HVaultConfig.Password = conf.StoreConfig.VaultConfig.Password
		}
		b, e := store.InitializeCassandraBackend(c)
		if e != nil {
			err = e
			log.WithError(err).Warnf("[EmailBroker] initalization of %s backend failed", conf.StoreName)
			return
		}

		broker.Store = backends.LDAStore(b) // type conversion to LDA interface
	default:
		log.Warnf("[EmailBroker] unknown store backend: %s", conf.StoreName)
		err = errors.New("[EmailBroker] unknown store backend")
		return
	}

	switch conf.IndexName {
	case "elasticsearch":
		c := index.ElasticSearchConfig{
			Urls: conf.IndexConfig.Urls,
		}
		i, e := index.InitializeElasticSearchIndex(c)
		if e != nil {
			err = e
			log.WithError(err).Warnf("[EmailBroker] initalization of %s backend failed", conf.IndexName)
			return
		}

		broker.Index = backends.LDAIndex(i) // type conversion to LDA interface
	}

	broker.NatsConn, e = nats.Connect(conf.NatsURL)
	if e != nil {
		err = e
		log.WithError(err).Warn("[EmailBroker] initalization of NATS connexion failed")
		return
	}
	switch conf.BrokerType {
	case "smtp":
		broker.Connectors.Ingress = make(chan *SmtpEmail)
		broker.Connectors.Egress = make(chan *SmtpEmail)

		e = broker.startIncomingSmtpAgents()
		if e != nil {
			err = e
			log.WithError(err).Warn("[EmailBroker] failed to start incoming smtp agent(s)")
			return
		}
		for i := 0; i < conf.NatsListeners; i++ {
			e = broker.startOutcomingSmtpAgents()
			if e != nil {
				err = e
				log.WithError(err).Warn("[EmailBroker] failed to start outcoming smtp agent(s)")
				return
			}
		}
		connectors = broker.Connectors
	case "imap":
		broker.Connectors.Ingress = make(chan *SmtpEmail)
		e = broker.startImapAgents()
		if e != nil {
			err = e
			log.WithError(err).Warn("[EmailBroker] failed to start imap agent(s)")
			return
		}
		connectors = broker.Connectors
	default:
		log.Errorf("[EmailBroker] initialize unknown broker type : <%s>", conf.BrokerType)
		return
	}
	caliopenConfig := CaliopenConfig{
		NotifierConfig: conf.NotifierConfig,
		NatsConfig: NatsConfig{
			Url:            conf.NatsURL,
			OutSMTP_topic:  conf.OutTopic,
			Contacts_topic: conf.ContactsTopic,
		},
		RESTstoreConfig: RESTstoreConfig{
			BackendName:  conf.StoreName,
			Consistency:  conf.StoreConfig.Consistency,
			Hosts:        conf.StoreConfig.Hosts,
			Keyspace:     conf.StoreConfig.Keyspace,
			OSSConfig:    conf.StoreConfig.OSSConfig,
			ObjStoreType: conf.StoreConfig.ObjectStore,
			SizeLimit:    conf.StoreConfig.SizeLimit,
		},
		RESTindexConfig: RESTIndexConfig{
			Hosts:     conf.IndexConfig.Urls,
			IndexName: conf.IndexName,
		},
	}
	broker.Notifier = Notifications.NewNotificationsFacility(caliopenConfig, broker.NatsConn)
	log.WithField("EmailBroker", conf.BrokerType).Info("EmailBroker started.")
	return
}

func (broker *EmailBroker) ShutDown() {
	for _, sub := range broker.natsSubscriptions {
		sub.Unsubscribe()
	}
	broker.NatsConn.Close()
	log.WithField("EmailBroker", "Nats subscriptions & connexion closed").Info()
	broker.Store.Close()
	log.WithField("EmailBroker", "Store client closed").Info()
	broker.Index.Close()
	log.WithField("EmailBroker", "Index client closed").Info()
}
