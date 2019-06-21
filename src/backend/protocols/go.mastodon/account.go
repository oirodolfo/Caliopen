// Copyleft (ɔ) 2018 The Caliopen contributors.
// Use of this source code is governed by a GNU AFFERO GENERAL PUBLIC
// license (AGPL) that can be found in the LICENSE file.

package mastodonworker

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	broker "github.com/CaliOpen/Caliopen/src/backend/brokers/go.mastodon"
	. "github.com/CaliOpen/Caliopen/src/backend/defs/go-objects"
	"github.com/CaliOpen/go-mastodon"
	log "github.com/Sirupsen/logrus"
	"github.com/dghubble/oauth1"
)

type (
	AccountHandler struct {
		AccountDesk    chan uint
		broker         *broker.MastodonBroker
		closed         bool
		lastDMseen     string
		MasterDesk     chan DeskMessage
		mastodonClient *mastodon.Client
		userAccount    *MastodonAccount
	}

	MastodonAccount struct {
		acct        string
		displayName string
		login       string
		mastodonID  string
		password    string
		remoteID    UUID
		url         string
		username    string
		userID      UUID
	}
)

const (
	//AccountDesk commands
	PollDM = uint(iota)
	PollTimeLine
	Stop

	lastSeenInfosKey = "lastseendm"
	lastSyncInfosKey = "lastsync"

	lastErrorKey      = "lastFetchError"
	dateFirstErrorKey = "firstErrorDate"
	dateLastErrorKey  = "lastErrorDate"
	errorsCountKey    = "errorsCount"
	syncingKey        = "syncing"

	defaultPollInterval = 10
	syncingTimeout      = 1 // how many hours to wait before restarting sync op
)

// NewAccountHandler creates a handler dedicated to a specific mastodon account.
// It caches remote identity credentials and data, as well as user context connection to mastodon API.
func NewAccountHandler(userID, remoteID string, worker Worker) (accountHandler *AccountHandler, err error) {
	accountHandler = new(AccountHandler)
	accountHandler.AccountDesk = make(chan uint)
	accountHandler.MasterDesk = worker.Desk
	b, e := broker.Initialize(worker.Conf.BrokerConfig, worker.Store, worker.Index, worker.NatsConn, worker.Notifier)
	if e != nil {
		err = fmt.Errorf("[MastodonAccount]NewAccountHandler failed to initialize a mastodon broker : %s", e)
		return nil, err
	}
	accountHandler.broker = b
	var remote *UserIdentity
	// retrieve data from db
	remote, err = accountHandler.broker.Store.RetrieveUserIdentity(userID, remoteID, true)
	if err != nil {
		log.WithError(err).Errorf("[MastodonAccount]NewAccountHandler failed to retrieve remote identity <%s> (user <%s>)", remoteID, userID)
		return
	}
	if remote.Credentials == nil {
		err = fmt.Errorf("[MastodonAccount]NewAccountHandler failed to retrieve credentials for remote identity <%s> (user <%s>)", remoteID, userID)
		return
	}
	accountHandler.userAccount = &MastodonAccount{
		login:           (*remote.Credentials)["token"],
		password:        (*remote.Credentials)["secret"],
		userID:          remote.UserId,
		remoteID:        remote.Id,
		accountUsername: remote.Identifier,
	}

	if lastseen, ok := remote.Infos[lastSeenInfosKey]; ok {
		accountHandler.lastDMseen = lastseen
	} else {
		accountHandler.lastDMseen = "0"
	}

	authConf := oauth1.NewConfig(worker.Conf.MastodonAppKey, worker.Conf.MastodonAppSecret)
	token := oauth1.NewToken(accountHandler.userAccount.accessToken, accountHandler.userAccount.accessTokenSecret)
	httpClient := authConf.Client(oauth1.NoContext, token)
	if accountHandler.mastodonClient = mastodon.NewClient(httpClient); accountHandler.mastodonClient == nil {
		return nil, errors.New("[NewWorker] mastodon api failed to create http client")
	}
	var mastodonID int64
	var screenName string
	accountHandler.usersScreenNames = map[int64]string{}

	// try to cache account's ID and screenName
	if mastodonid, ok := remote.Infos["mastodonid"]; ok && mastodonid != "" {
		accountHandler.userAccount.mastodonID = mastodonid
		mastodonID, _ = strconv.ParseInt(mastodonid, 10, 64)
		if mastodonname, ok := remote.Infos["screen_name"]; ok && mastodonname != "" {
			screenName = mastodonname
		} else {
			screenName = accountHandler.getAccountName(mastodonid)
		}
	} else {
		mastodonUser, _, e := accountHandler.mastodonClient.Users.Show(&mastodon.UserShowParams{ScreenName: accountHandler.userAccount.screenName})
		if e == nil {
			accountHandler.userAccount.mastodonID = mastodonUser.IDStr
			mastodonID = mastodonUser.ID
			screenName = mastodonUser.ScreenName
		}
	}
	if mastodonID != 0 && screenName != "" {
		accountHandler.usersScreenNames[mastodonID] = screenName
	}
	return
}

// Start begins infinite loops, until receiving stop order. This func must be call within goroutine.
func (worker *AccountHandler) Start() {
	go func(w *AccountHandler) {
		for worker.broker != nil {
			select {
			case egress, ok := <-w.broker.Connectors.Egress:
				if !ok {
					if !worker.closed {
						close(worker.broker.Connectors.Halt)
						close(worker.AccountDesk)
						worker.closed = true
					}
					worker.MasterDesk <- DeskMessage{closeAccountOrder, worker}
					return
				}
				err := w.SendDM(egress.Order)
				if err != nil {
					egress.Ack <- &DeliveryAck{
						Err:      true,
						Response: err.Error(),
					}
				} else {
					egress.Ack <- &DeliveryAck{
						Err:      false,
						Response: "OK",
					}
				}
			case <-w.broker.Connectors.Halt:
				worker.MasterDesk <- DeskMessage{closeAccountOrder, worker}
				return
			}
		}
	}(worker)

	for command := range worker.AccountDesk {
		switch command {
		case PollDM:
			worker.PollDM()
		case Stop:
			worker.Stop()
			return
		default:
			log.Warnf("worker received unknown command number %d", command)
		}
	}
}

func (worker *AccountHandler) Stop() {
	if !worker.closed {
		close(worker.broker.Connectors.Egress)
		close(worker.broker.Connectors.Halt)
		close(worker.AccountDesk)
		worker.closed = true
	}
}

// PollDM calls Mastodon API endpoint to fetch DMs
// it passes unseen DM to its embedded broker
func (worker *AccountHandler) PollDM() {
	// retrieve user_identity.infos
	accountInfos, retrieveErr := worker.broker.Store.RetrieveRemoteInfosMap(worker.userAccount.userID.String(), worker.userAccount.remoteID.String())
	if retrieveErr != nil {
		log.WithError(retrieveErr).Warnf("[AccountHandler %s] PollDM failed to retrieve infos map", worker.userAccount.remoteID.String())
		return
	}
	// check if a sync process is running
	if syncing, ok := accountInfos[syncingKey]; ok && syncing != "" {
		startDate, e := time.Parse(time.RFC3339, syncing)
		if e == nil && time.Since(startDate)/time.Hour < syncingTimeout {
			log.Infof("[PollDM] avoiding concurrent sync for <%s>. Syncing in progress since %s", worker.userAccount.remoteID, accountInfos["syncing"])
			return
		}
	}
	// save syncing state in db to prevent concurrent sync
	accountInfos[syncingKey] = time.Now().Format(time.RFC3339)
	err := worker.broker.Store.UpdateRemoteInfosMap(worker.userAccount.userID.String(), worker.userAccount.remoteID.String(), accountInfos)
	if err != nil {
		log.WithError(err).Infof("[PollDM] failed to update syncing state user <%s>, identity <%s>", worker.userAccount.userID, worker.userAccount.remoteID)
		return
	}

	// do not forget to always write down last_check timestamp
	// and to remove syncing state before leaving
	defer func() {
		if worker.broker != nil {
			delete(accountInfos, lastErrorKey)
			delete(accountInfos, errorsCountKey)
			delete(accountInfos, dateFirstErrorKey)
			delete(accountInfos, dateLastErrorKey)
			delete(accountInfos, syncingKey)
			e := worker.broker.Store.UpdateRemoteInfosMap(worker.userAccount.userID.String(), worker.userAccount.remoteID.String(), accountInfos)
			if e != nil {
				log.WithError(e).Warnf("[AccountHandler %s] PollDM failed to update sync state in db", worker.userAccount.remoteID.String())
			}
			log.Infof("[AccountHandler %s] PollDM finished", worker.userAccount.remoteID.String())
			e = worker.broker.Store.TimestampRemoteLastCheck(worker.userAccount.userID.String(), worker.userAccount.remoteID.String())
			if e != nil {
				log.WithError(e).Warnf("[AccountHandler %s] PollDM failed to update last_check state in db", worker.userAccount.remoteID.String())
			}
		}
	}()

	// retrieve DM list from mastodon API
	DMs, _, err := worker.mastodonClient.DirectMessages.EventsList(&mastodon.DirectMessageEventsListParams{Count: 50})
	if err != nil {
		if e, ok := err.(mastodon.APIError); ok {
			errorsMessages := new(strings.Builder)
			for _, err := range e.Errors {
				switch err.Code {
				case 88: // rate limit error => send throttling order to idpoller
					var interval int
					log.Infof("[AccountHandler %s] PollDM : mastodon returned rate limit error, slowing down worker for account", worker.userAccount.remoteID)
					if pollInterval, ok := accountInfos["pollinterval"]; ok {
						interval, e := strconv.Atoi(pollInterval)
						if e == nil {
							interval *= 2
							// prevent boundaries overflow : min = 1 min, max = 3 days
							if interval < 1 || interval > 3*24*60 {
								interval = defaultPollInterval
							}
						} else {
							interval = defaultPollInterval
						}
					} else {
						interval = defaultPollInterval
					}
					newInterval := strconv.Itoa(interval)
					accountInfos["pollinterval"] = newInterval
					e := worker.broker.Store.UpdateRemoteInfosMap(worker.userAccount.userID.String(), worker.userAccount.remoteID.String(), accountInfos)
					if e != nil {
						log.WithError(e).Warnf("[AccountHandler %s] PollDM : failed to updateRemoteInfosMap with new poll interval", worker.userAccount.userID.String()+"/"+worker.userAccount.remoteID.String())
					}
					order := RemoteIDNatsMessage{
						IdentityId: worker.userAccount.remoteID.String(),
						Order:      "update_interval",
						OrderParam: newInterval,
						Protocol:   "mastodon",
						UserId:     worker.userAccount.userID.String(),
					}
					jorder, jerr := json.Marshal(order)
					if jerr == nil {
						e := worker.broker.NatsConn.Publish(worker.broker.Config.NatsTopicPollerCache, jorder)
						if e != nil {
							log.WithError(e).Warnf("[AccountHandler %s] PollDM : failed to publish new poll interval to idpoller", worker.userAccount.userID.String()+"/"+worker.userAccount.remoteID.String())
						}
					}
				case 89: // invalid token or token expired. Suicide this accountworker thus it will be re-created with new oauth next time idpoller will order it
					delete(accountInfos, syncingKey)
					e := worker.saveErrorState(accountInfos, errorsMessages.String())
					if e != nil {
						log.WithError(e).Warnf("[AccountHandler %s] PollDM failed to update sync state in db", worker.userAccount.remoteID.String())
					}
					worker.MasterDesk <- DeskMessage{closeAccountOrder, worker}
					return
				}
				errorsMessages.WriteString(err.Message + " ")
			}
			e := worker.saveErrorState(accountInfos, errorsMessages.String())
			if e != nil {
				log.WithError(e).Warnf("[AccountHandler %s] PollDM failed to update sync state in db", worker.userAccount.remoteID.String())
			}
			return

		} else {
			e := worker.saveErrorState(accountInfos, err.Error())
			if e != nil {
				log.WithError(e).Warnf("[AccountHandler %s] PollDM failed to update sync state in db", worker.userAccount.remoteID.String())
			}
			return
		}
	}

	sort.Sort(ByAscID(DMs.Events)) // reverse events order to get older DMs first

	if len(DMs.Events) > 0 && worker.dmNotSeen(DMs.Events[0]) {
		//TODO: handle pagination with `cursor` param
	}

	log.Infof("[AccountHandler %s] PollDM %d events retrieved", worker.userAccount.remoteID.String(), len(DMs.Events))
	for _, event := range DMs.Events {
		if worker.dmNotSeen(event) {
			//lookup sender & recipient's screen_names because there are not embedded in event object
			accountName := worker.getAccountName(event.Message.SenderID)
			if accountName == "" {
				continue // we don't want to inject a message with an incomplete participant
			}
			(*event.Message).SenderScreenName = accountName
			accountName = worker.getAccountName(event.Message.Target.RecipientID)
			if accountName == "" {
				continue // we don't want to inject a message with an incomplete participant
			}
			(*event.Message).Target.RecipientScreenName = accountName

			err = worker.broker.ProcessInDM(worker.userAccount.userID, worker.userAccount.remoteID, &event, true)
			if err != nil {
				// something went wrong, forget this DM
				log.WithError(err).Warnf("[AccountHandler %s] ProcessInDM failed for event : %+v", worker.userAccount.remoteID.String(), event)
				continue
			}
			worker.lastDMseen = event.ID
			// update sync status in db
			// TODO: algorithm to shorten pollinterval after new DM has been received
			accountInfos[lastSeenInfosKey] = event.ID
			accountInfos[lastSyncInfosKey] = time.Now().Format(time.RFC3339)
			err = worker.broker.Store.UpdateRemoteInfosMap(worker.userAccount.userID.String(), worker.userAccount.remoteID.String(), accountInfos)
			if err != nil {
				log.WithError(err).Warnf("[AccountHandler %s] ProcessInDM failed to update InfosMap for event : %+v", worker.userAccount.remoteID.String(), event)
				continue
			}
		}
	}

}

func (worker *AccountHandler) dmNotSeen(event mastodon.DirectMessageEvent) bool {
	return worker.lastDMseen < event.ID
}

// SendDM delivers DM to Mastodon endpoint and give back Mastodon's response to broker.
func (worker *AccountHandler) SendDM(order BrokerOrder) error {
	// make use of broker to marshal a direct message
	brokerPort := make(chan *broker.DMpayload)
	var brokerMessage *broker.DMpayload

	go worker.broker.ProcessOutDM(order, brokerPort)

	select {
	case brokerMessage = <-brokerPort:
		if brokerMessage.Err != nil {
			return brokerMessage.Err
		}
	case <-time.After(10 * time.Second):
		return errors.New("[SendDM] broker timeout")
	}

	// retrieve recipient's mastodon ID from DM's screenName
	user, _, userErr := worker.mastodonClient.Users.Show(&mastodon.UserShowParams{
		ScreenName: brokerMessage.DM.Message.Target.RecipientScreenName,
	})
	if userErr != nil {
		brokerMessage.Response <- broker.MastodonDeliveryAck{
			Err:      true,
			Response: userErr.Error(),
		}
		return userErr
	}
	brokerMessage.DM.Message.Target.RecipientID = user.IDStr

	// deliver DM through Mastodon API
	createResponse, _, errResponse := worker.mastodonClient.DirectMessages.EventsCreate(brokerMessage.DM.Message)
	if errResponse != nil {
		brokerMessage.Response <- broker.MastodonDeliveryAck{
			Payload:  createResponse,
			Err:      true,
			Response: errResponse.Error(),
		}
		return errResponse
	}

	// give back Mastodon's reply to broker for it finishes its job
	brokerMessage.Response <- broker.MastodonDeliveryAck{
		Payload: createResponse,
	}

	select {
	case brokerMessage = <-brokerPort:
		if brokerMessage.Err != nil {
			return brokerMessage.Err
		}
		return nil
	case <-time.After(10 * time.Second):
		return errors.New("[SendDM] broker timeout")
	}
}

// getAccountName returns Mastodon account screen name given a Mastodon account ID
// screen name is retrieve either from worker's cache or Mastodon API
// returns empty string if it fails.
func (worker *AccountHandler) getAccountName(accountID string) (accountName string) {
	ID, err := strconv.ParseInt(accountID, 10, 64)
	if err == nil {
		var inCache bool
		if accountName, inCache = worker.usersScreenNames[ID]; !inCache {
			user, resp, err := worker.mastodonClient.Users.Show(&mastodon.UserShowParams{UserID: ID})
			if err == nil && user != nil {
				(*worker).usersScreenNames[ID] = user.ScreenName
				return user.ScreenName
			} else {
				log.WithError(err).Warnf("[AccountHandler] failed to getAccountName for mastodon ID %s. Got user {%+v} and http response {%+v}", accountID, user, resp)
			}
		}
		return accountName
	}
	return
}

// isDMUnique returns true if Mastodon Direct Message id is not found within user's messages index
// if seeking fails for any reason, true is returned anyway to allow duplication
func (worker *AccountHandler) isDMUnique(dmID string) bool {
	messageID, err := worker.broker.Store.SeekMessageByExternalRef(worker.userAccount.userID.String(), dmID, "")
	if err != nil || bytes.Equal(messageID.Bytes(), EmptyUUID.Bytes()) {
		return true
	}
	return false
}

func (worker *AccountHandler) saveErrorState(infos map[string]string, err string) error {

	// ensure errors data fields are present
	if _, ok := infos[lastErrorKey]; !ok {
		infos[lastErrorKey] = ""
	}
	if _, ok := infos[dateFirstErrorKey]; !ok {
		infos[dateFirstErrorKey] = ""
	}
	if _, ok := infos[dateLastErrorKey]; !ok {
		infos[dateLastErrorKey] = ""
	}
	if _, ok := infos[errorsCountKey]; !ok {
		infos[errorsCountKey] = "0"
	}

	// log last error
	infos[lastErrorKey] = "Mastodon connection failed : " + err
	log.Warnf("Mastodon connection failed for remote identity %s : %s", worker.userAccount.remoteID, err)
	// increment counter
	count, _ := strconv.Atoi(infos[errorsCountKey])
	count++
	infos[errorsCountKey] = strconv.Itoa(count)

	// update dates
	lastDate := time.Now()
	var firstDate time.Time
	firstDate, _ = time.Parse(time.RFC3339, infos[dateFirstErrorKey])
	if firstDate.IsZero() {
		firstDate = lastDate
	}
	infos[dateFirstErrorKey] = firstDate.Format(time.RFC3339)
	infos[dateLastErrorKey] = lastDate.Format(time.RFC3339)

	// check failuresThreshold
	if lastDate.Sub(firstDate)/time.Hour > failuresThreshold {
		// disable remote identity
		err := worker.broker.Store.UpdateUserIdentity(&UserIdentity{
			UserId: worker.userAccount.userID,
			Id:     worker.userAccount.remoteID,
		}, map[string]interface{}{
			"Status": "inactive",
		})
		if err != nil {
			log.WithError(err).Warnf("[saveErrorState] failed to deactivate remote identity %s for user %s", worker.userAccount.remoteID, worker.userAccount.userID)
		}
		// send nats message to idpoller to stop polling
		order := RemoteIDNatsMessage{
			IdentityId: worker.userAccount.remoteID.String(),
			Order:      "delete",
			Protocol:   "mastodon",
			UserId:     worker.userAccount.userID.String(),
		}
		jorder, jerr := json.Marshal(order)
		if jerr == nil {
			e := worker.broker.NatsConn.Publish(worker.broker.Config.NatsTopicPollerCache, jorder)
			if e != nil {
				log.WithError(e).Warnf("[saveErrorState] failed to publish delete order to idpoller")
			}
		}
	}

	// udpate UserIdentity in db
	return worker.broker.Store.UpdateRemoteInfosMap(worker.userAccount.userID.String(), worker.userAccount.remoteID.String(), infos)

}

// ByAscID implements sort interface
type ByAscID []mastodon.DirectMessageEvent

func (bri ByAscID) Len() int {
	return len(bri)
}

func (bri ByAscID) Less(i, j int) bool {
	return bri[i].ID < bri[j].ID
}

func (bri ByAscID) Swap(i, j int) {
	bri[i], bri[j] = bri[j], bri[i]
}
