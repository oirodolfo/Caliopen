package mastodonworker

import (
	"errors"
	"fmt"
	broker "github.com/CaliOpen/Caliopen/src/backend/brokers/go.mastodon"
	. "github.com/CaliOpen/Caliopen/src/backend/defs/go-objects"
	"github.com/CaliOpen/Caliopen/src/backend/main/go.backends"
	"github.com/CaliOpen/Caliopen/src/backend/main/go.backends/index/elasticsearch"
	"github.com/CaliOpen/Caliopen/src/backend/main/go.backends/store/cassandra"
	"github.com/CaliOpen/Caliopen/src/backend/main/go.main/facilities/Notifications"
	log "github.com/Sirupsen/logrus"
	"github.com/gocql/gocql"
	"github.com/nats-io/go-nats"
	"sync"
	"time"
)

type (
	Worker struct {
		AccountHandlers map[string]*AccountHandler // one worker per active Mastodon account
		HaltGroup       *sync.WaitGroup
		Index           backends.LDAIndex
		Id              string
		NatsConn        *nats.Conn
		NatsSubs        []*nats.Subscription
		Notifier        *Notifications.Notifier
		Store           backends.LDAStore
		WorkersGuard    *sync.RWMutex
		Conf            WorkerConfig
		Desk            chan DeskMessage // chan to allow accountworkers to communicate with their master
	}

	WorkerConfig struct {
		Workers           uint8               `mapstructure:"workers"`
		MastodonAppKey    string              `mapstructure:"mastodon_app_key"`
		MastodonAppSecret string              `mapstructure:"mastodon_app_secret"`
		BrokerConfig      broker.BrokerConfig `mapstructure:"BrokerConfig"`
	}

	DeskMessage struct {
		order   string
		account *AccountHandler
	}
)

const (
	failuresThreshold = 72 // how many hours to wait before disabling a faulty remote.
	noPendingJobErr   = "no pending job"
	pollThrottling    = 10 * time.Second
	needJobOrderStr   = `{"worker":"%s","order":{"order":"need_job"}}`
	closeAccountOrder = "close_account"
)

func InitWorker(conf WorkerConfig, verboseLog bool, id string) (worker *Worker, err error) {

	if verboseLog {
		log.SetLevel(log.DebugLevel)
	}

	worker = &Worker{
		AccountHandlers: map[string]*AccountHandler{},
		Conf:            conf,
		Id:              id,
		WorkersGuard:    new(sync.RWMutex),
	}

	// init Store
	switch conf.BrokerConfig.StoreName {
	case "cassandra":
		c := store.CassandraConfig{
			Hosts:       conf.BrokerConfig.StoreConfig.Hosts,
			Keyspace:    conf.BrokerConfig.StoreConfig.Keyspace,
			Consistency: gocql.Consistency(conf.BrokerConfig.StoreConfig.Consistency),
			SizeLimit:   conf.BrokerConfig.StoreConfig.SizeLimit,
			UseVault:    conf.BrokerConfig.StoreConfig.UseVault,
		}
		if conf.BrokerConfig.StoreConfig.ObjectStore == "s3" {
			c.WithObjStore = true
			c.Endpoint = conf.BrokerConfig.StoreConfig.OSSConfig.Endpoint
			c.AccessKey = conf.BrokerConfig.StoreConfig.OSSConfig.AccessKey
			c.SecretKey = conf.BrokerConfig.StoreConfig.OSSConfig.SecretKey
			c.RawMsgBucket = conf.BrokerConfig.StoreConfig.OSSConfig.Buckets["raw_messages"]
			c.AttachmentBucket = conf.BrokerConfig.StoreConfig.OSSConfig.Buckets["temporary_attachments"]
			c.Location = conf.BrokerConfig.StoreConfig.OSSConfig.Location
		}
		if conf.BrokerConfig.StoreConfig.UseVault {
			c.HVaultConfig.Url = conf.BrokerConfig.StoreConfig.VaultConfig.Url
			c.HVaultConfig.Username = conf.BrokerConfig.StoreConfig.VaultConfig.Username
			c.HVaultConfig.Password = conf.BrokerConfig.StoreConfig.VaultConfig.Password
		}
		b, e := store.InitializeCassandraBackend(c)
		if e != nil {
			err = e
			log.WithError(err).Warnf("[MastodonWorker] initialization of %s backend failed", conf.BrokerConfig.StoreName)
			return
		}

		worker.Store = backends.LDAStore(b) // type conversion to LDA interface
	default:
		log.Warnf("[MastodonWorker] unknown store backend: %s", conf.BrokerConfig.StoreName)
		err = errors.New("[MastodonWorker] unknown store backend")
		return

	}

	// init Index
	switch conf.BrokerConfig.LDAConfig.IndexName {
	case "elasticsearch":
		c := index.ElasticSearchConfig{
			Urls: conf.BrokerConfig.LDAConfig.IndexConfig.Urls,
		}
		i, e := index.InitializeElasticSearchIndex(c)
		if e != nil {
			err = e
			log.WithError(err).Warnf("[MastodonBroker] initialization of %s backend failed", conf.BrokerConfig.IndexName)
			return
		}

		worker.Index = backends.LDAIndex(i) // type conversion to LDA interface
	default:
		log.Warnf("[MastodonBroker] unknown index backend: %s", conf.BrokerConfig.LDAConfig.IndexName)
		err = errors.New("[MastodonBroker] unknown index backend")
		return
	}

	worker.NatsConn, err = nats.Connect(conf.BrokerConfig.NatsURL)
	if err != nil {
		log.WithError(err).Warn("[MastodonBroker] initalization of NATS connexion failed")
		return
	}
	caliopenConfig := CaliopenConfig{
		NotifierConfig: conf.BrokerConfig.LDAConfig.NotifierConfig,
		NatsConfig: NatsConfig{
			Url: conf.BrokerConfig.NatsURL,
		},
		RESTstoreConfig: RESTstoreConfig{
			BackendName:  conf.BrokerConfig.StoreName,
			Consistency:  conf.BrokerConfig.StoreConfig.Consistency,
			Hosts:        conf.BrokerConfig.StoreConfig.Hosts,
			Keyspace:     conf.BrokerConfig.StoreConfig.Keyspace,
			OSSConfig:    conf.BrokerConfig.StoreConfig.OSSConfig,
			ObjStoreType: conf.BrokerConfig.StoreConfig.ObjectStore,
			SizeLimit:    conf.BrokerConfig.StoreConfig.SizeLimit,
		},
		RESTindexConfig: RESTIndexConfig{
			Hosts:     conf.BrokerConfig.LDAConfig.IndexConfig.Urls,
			IndexName: conf.BrokerConfig.LDAConfig.IndexName,
		},
	}
	worker.Notifier = Notifications.NewNotificationsFacility(caliopenConfig, worker.NatsConn)

	// init Nats connector
	worker.NatsConn, err = nats.Connect(conf.BrokerConfig.NatsURL)
	if err != nil {
		log.WithError(err).Fatal("[MastodonWorker] initialization of NATS connexion failed")
	}
	worker.NatsSubs = make([]*nats.Subscription, 1)
	worker.NatsSubs[0], err = worker.NatsConn.QueueSubscribe(conf.BrokerConfig.NatsTopicDMs, conf.BrokerConfig.NatsQueue, worker.DMmsgHandler)
	if err != nil {
		log.WithError(err).Fatal("[MastodonWorker] initialization of NATS fetcher subscription failed")
	}
	err = worker.NatsConn.Flush()
	if err != nil {
		log.WithError(err).Fatal("[MastodonWorker] initialization of NATS fetcher subscription failed")
	}

	worker.Desk = make(chan DeskMessage, 2)

	return worker, nil
}

func (worker *Worker) Start(throttling ...time.Duration) {
	var throttle time.Duration
	if len(throttling) == 1 && throttling[0] != 0 {
		throttle = throttling[0]
	} else {
		throttle = pollThrottling
	}

	go func() {
		for msg := range worker.Desk {
			switch msg.order {
			case closeAccountOrder:
				if msg.account != nil {
					worker.RemoveAccountHandler(msg.account)
				}
			default:
				log.Debugf("[MastodonWorker] received unknown order « %s » from account %s", msg.order, msg.account.userAccount.userID.String()+msg.account.userAccount.remoteID.String())
			}
		}
	}()

	// start throttled jobs polling
	log.Infof("Mastodon worker %s starting with %d sec throttling", worker.Id, throttle/time.Second)
	for {
		start := time.Now()
		requestOrder := []byte(fmt.Sprintf(needJobOrderStr, worker.Id))
		log.Infof("Mastodon worker %s is requesting jobs to idpoller", worker.Id)
		resp, err := worker.NatsConn.Request(worker.Conf.BrokerConfig.NatsTopicPoller, requestOrder, time.Minute)
		if err != nil {
			log.WithError(err).Warnf("[worker %s] failed to request pending jobs on nats", worker.Id)
		} else {
			worker.WorkerMsgHandler(resp)
		}
		// check for interrupt after job is finished
		if worker.HaltGroup != nil {
			worker.stop()
			break
		}
		elapsed := time.Now().Sub(start)
		if elapsed < throttle {
			time.Sleep(throttle - elapsed)
		}
	}
}

func (worker *Worker) stop() {
	for _, w := range worker.AccountHandlers {
		w.AccountDesk <- Stop
	}
	for _, sub := range worker.NatsSubs {
		sub.Unsubscribe()
	}
	worker.NatsConn.Close()
	worker.Store.Close()
	worker.Index.Close()
	worker.HaltGroup.Done()
	close(worker.Desk)
	log.Infof("worker %s stopped", worker.Id)
}

// getOrCreateHandler returns a pointer to a worker already in cache
// or tries to create a new worker for the remote identity if not.
// returns nil if get or create failed.
func (w *Worker) getOrCreateHandler(userId, remoteId string) *AccountHandler {
	w.WorkersGuard.RLock()
	if accountHandler, ok := w.AccountHandlers[userId+remoteId]; ok {
		w.WorkersGuard.RUnlock()
		return accountHandler
	} else {
		w.WorkersGuard.RUnlock()
		log.Infof("[getOrCreateHandler] failed to retrieve registered worker for remote %s (user %s). Trying to add one.", remoteId, userId)
		if userId == "" || remoteId == "" {
			return nil
		}
		accountHandler, err := NewAccountHandler(userId, remoteId, *w)
		if err != nil {
			log.WithError(err).Warnf("[getOrCreateHandler] failed to create new worker for remote %s (user %s)", remoteId, userId)
			return nil
		}
		w.RegisterAccountHandler(accountHandler)
		go accountHandler.Start()
		return accountHandler

	}
}

func (w *Worker) RegisterAccountHandler(accountHandler *AccountHandler) {
	workerKey := accountHandler.userAccount.userID.String() + accountHandler.userAccount.remoteID.String()
	// stop & remove handler first if it's already registered
	w.WorkersGuard.RLock()
	registeredHandler, ok := w.AccountHandlers[workerKey]
	w.WorkersGuard.RUnlock()
	if ok {
		w.RemoveAccountHandler(registeredHandler)
	}
	w.WorkersGuard.Lock()
	w.AccountHandlers[workerKey] = accountHandler
	w.WorkersGuard.Unlock()
}

func (w *Worker) RemoveAccountHandler(accountHandler *AccountHandler) {
	workerKey := accountHandler.userAccount.userID.String() + accountHandler.userAccount.remoteID.String()
	w.WorkersGuard.Lock()
	accountHandler.Stop()
	delete(w.AccountHandlers, workerKey)
	w.WorkersGuard.Unlock()
}
