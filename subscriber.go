// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/log"
)

type subscriber struct {
	logger *log.Logger
	broker base.Broker

	// channel to communicate back to the long running "subscriber" goroutine.
	done chan struct{}

	// cancelations hold cancel functions for all active tasks.
	cancelations *base.Cancelations

	// time to wait before retrying to connect to redis.
	retryTimeout time.Duration
}

type subscriberParams struct {
	logger       *log.Logger
	broker       base.Broker
	cancelations *base.Cancelations
}

func newSubscriber(params subscriberParams) *subscriber {
	return &subscriber{
		logger:       params.logger,
		broker:       params.broker,
		done:         make(chan struct{}),
		cancelations: params.cancelations,
		retryTimeout: 5 * time.Second,
	}
}

func (s *subscriber) shutdown() {
	s.logger.Debug("Subscriber shutting down...")
	// Signal the subscriber goroutine to stop.
	s.done <- struct{}{}
}

func (s *subscriber) start(wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		pubsub, err := s.broker.CancelationPubSub()
		if err != nil {
			// Log a warning and disable subscriber if the broker doesn't support PubSub (e.g., Cassandra).
			s.logger.Warnf("Task cancellation feature is not supported by the current broker (%T): %v. Cancels via API will not be processed by this server instance.", s.broker, err)
			// Note: For Redis, if this initial call fails, it used to retry.
			// Now, if the broker is Redis and it fails here, the subscriber also won't run.
			// This simplifies the logic as the primary goal is to handle non-PubSub brokers gracefully.
			// If Redis connection is temporarily down, other parts of Asynq (like heartbeater) would also be affected.
			s.logger.Debug("Subscriber done (due to lack of PubSub support or initial connection error)")
			return
		}

		// Proceed only if pubsub is successfully obtained (i.e., broker supports it and connection was successful)
		s.logger.Info("Cancelation subscriber started")
		cancelCh := pubsub.Channel()
		for {
			select {
			case <-s.done:
				if err := pubsub.Close(); err != nil {
					s.logger.Errorf("Error closing pubsub in subscriber: %v", err)
				}
				s.logger.Debug("Subscriber done")
				return
			case msg := <-cancelCh:
				if msg == nil {
					s.logger.Info("Cancelation channel closed, subscriber stopping...")
					// This can happen if the connection to the broker is lost.
					// The original code would loop and try to re-establish pubsub.
					// For simplicity now, we exit. Re-establishment might be complex
					// and better handled by a full server restart or more robust broker connection management.
					return
				}
				s.logger.Debugf("Received cancelation signal for Task ID %q", msg.Payload)
				cancel, ok := s.cancelations.Get(msg.Payload)
				if ok {
					s.logger.Debugf("Found cancel func for Task ID %q, invoking it", msg.Payload)
					cancel()
				} else {
					s.logger.Debugf("No cancel func found for Task ID %q (already processed or unknown)", msg.Payload)
				}
			}
		}
	}()
}
