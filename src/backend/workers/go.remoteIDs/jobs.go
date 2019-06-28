// Copyleft (ɔ) 2019 The Caliopen contributors.
// Use of this source code is governed by a GNU AFFERO GENERAL PUBLIC
// license (AGPL) that can be found in the LICENSE file.

package go_remoteIDs

import (
	"crypto/sha256"
	"errors"
	"fmt"
	. "github.com/CaliOpen/Caliopen/src/backend/defs/go-objects"
	log "github.com/Sirupsen/logrus"
	"sync"
)

type JobsHandler struct {
	pendingJobs  map[string]map[[32]byte]Job // pattern is [worker][job-hash]job
	jobsSequence map[string][][32]byte       // pattern is [worker][]job-hash <- FIFO ordered
	jobsMux      *sync.Mutex
}

type Job struct {
	Worker string // protocol worker like email, twitter, etc. as defined in idpoller's config
	Order  BrokerOrder
}

func initJobsHandler() (*JobsHandler, error) {
	return &JobsHandler{
		map[string]map[[32]byte]Job{},
		map[string][][32]byte{},
		&sync.Mutex{},
	}, nil
}

// AddPendingJob adds a job to _pending_ map only if job's hash does not already exists for job's worker.
// pending jobs are ordered in a FIFO list per each worker.
func (jh *JobsHandler) AddPendingJob(job Job) {
	jh.jobsMux.Lock()
	key := sha256.Sum256([]byte(job.Order.UserId + job.Order.IdentityId + job.Order.Order))
	if _, ok := jh.pendingJobs[job.Worker]; !ok {
		jh.pendingJobs[job.Worker] = make(map[[32]byte]Job)
	}
	if _, ok := jh.pendingJobs[job.Worker][key]; !ok {
		jh.pendingJobs[job.Worker][key] = job
		jh.jobsSequence[job.Worker] = append(jh.jobsSequence[job.Worker], key)
	}
	log.Debugf("pending jobs list : %+v\n", jh.pendingJobs)
	log.Debugf("jobs sequence list : %+v\n", jh.jobsSequence)
	log.Infof("[jobsHandler] jobs for workers <%s> updated : %d job(s) pending", job.Worker, len(jh.jobsSequence[job.Worker]))
	jh.jobsMux.Unlock()
}

// ConsumePendingJobFor returns first-in pending job for worker and removes it from lists
// returns error if no pending job
func (jh *JobsHandler) ConsumePendingJobFor(worker string) (Job, error) {
	jh.jobsMux.Lock()
	defer jh.jobsMux.Unlock()
	if len(jh.jobsSequence[worker]) == 0 {
		return Job{}, errors.New(noPendingJobErr)
	}
	// get most ancient job for worker
	job := jh.pendingJobs[worker][jh.jobsSequence[worker][0]]
	// remove job from pending queue
	delete(jh.pendingJobs[worker], jh.jobsSequence[worker][0])
	// truncate jobsSequence list
	jh.jobsSequence[worker] = jh.jobsSequence[worker][1:]

	log.Infof("[jobsHandler] 1 <%s> job consumed  : %d job(s) pending", job.Worker, len(jh.jobsSequence[job.Worker]))
	log.Debugf("pending jobs list : %+v", jh.pendingJobs)
	log.Debugf("jobs sequence list : %+v", jh.jobsSequence)
	return job, nil
}

// Run implements cron.Job interface to add job to relevant worker's list
func (j Job) Run() {
	poller.jobs.AddPendingJob(j)
}

func buildSyncJob(entry cacheEntry) (Job, error) {
	var job Job

	// need a switch to avoid copying remote protocol directly into worker's name because of legacy deprecated protocol name 'imap'
	switch entry.remoteProtocol {
	case "email", "imap":
		job.Worker = imapWorker
	case "twitter":
		job.Worker = twitterWorker
	case "mastodon":
		job.Worker = mastodonWorker
	default:
		return Job{}, fmt.Errorf("unhandled remote protocol : %s", entry.remoteProtocol)
	}
	job.Order = BrokerOrder{
		Order:      "sync",
		UserId:     entry.userID.String(),
		IdentityId: entry.remoteID.String(),
	}
	log.Debugf("new job built : %+v", job)
	return job, nil
}
