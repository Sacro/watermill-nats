package jetstream

import (
	"context"
	"sync"
	"time"

	internalSync "github.com/ThreeDotsLabs/watermill/pubsub/sync"

	nats "github.com/nats-io/nats.go"
	"github.com/pkg/errors"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

type StreamingSubscriberConfig struct {
	// ClusterID is the NATS Streaming cluster ID.
	ClusterID string

	// ClientID is the NATS Streaming client ID to connect with.
	// ClientID can contain only alphanumeric and `-` or `_` characters.
	//
	// Using DurableName causes the NATS Streaming server to track
	// the last acknowledged message for that ClientID + DurableName.
	ClientID string

	// QueueGroup is the NATS Streaming queue group.
	//
	// All subscriptions with the same queue name (regardless of the connection they originate from)
	// will form a queue group. Each message will be delivered to only one subscriber per queue group,
	// using queuing semantics.
	//
	// It is recommended to set it with DurableName.
	// For non durable queue subscribers, when the last member leaves the group,
	// that group is removed. A durable queue group (DurableName) allows you to have all members leave
	// but still maintain state. When a member re-joins, it starts at the last position in that group.
	//
	// When QueueGroup is empty, subscribe without QueueGroup will be used.
	QueueGroup string

	// DurableName is the NATS streaming durable name.
	//
	// Subscriptions may also specify a “durable name” which will survive client restarts.
	// Durable subscriptions cause the server to track the last acknowledged message
	// sequence number for a client and durable name. When the client restarts/resubscribes,
	// and uses the same client ID and durable name, the server will resume delivery beginning
	// with the earliest unacknowledged message for this durable subscription.
	//
	// Doing this causes the NATS Streaming server to track
	// the last acknowledged message for that ClientID + DurableName.
	DurableName string

	// SubscribersCount determines wow much concurrent subscribers should be started.
	SubscribersCount int

	// CloseTimeout determines how long subscriber will wait for Ack/Nack on close.
	// When no Ack/Nack is received after CloseTimeout, subscriber will be closed.
	CloseTimeout time.Duration

	// How long subscriber should wait for Ack/Nack. When no Ack/Nack was received, message will be redelivered.
	// It is mapped to stan.AckWait option.
	AckWaitTimeout time.Duration

	// NatsOptions are custom []nats.Option passed to the connection.
	// It is also used to provide connection parameters, for example:
	// 		nats.NatsURL("nats://localhost:4222")
	NatsOptions []nats.Option

	// Unmarshaler is an unmarshaler used to unmarshaling messages from NATS format to Watermill format.
	Unmarshaler Unmarshaler
}

type StreamingSubscriberSubscriptionConfig struct {
	// Unmarshaler is an unmarshaler used to unmarshaling messages from NATS format to Watermill format.
	Unmarshaler Unmarshaler
	// QueueGroup is the NATS Streaming queue group.
	//
	// All subscriptions with the same queue name (regardless of the connection they originate from)
	// will form a queue group. Each message will be delivered to only one subscriber per queue group,
	// using queuing semantics.
	//
	// It is recommended to set it with DurableName.
	// For non durable queue subscribers, when the last member leaves the group,
	// that group is removed. A durable queue group (DurableName) allows you to have all members leave
	// but still maintain state. When a member re-joins, it starts at the last position in that group.
	//
	// When QueueGroup is empty, subscribe without QueueGroup will be used.
	QueueGroup string

	// DurableName is the NATS streaming durable name.
	//
	// Subscriptions may also specify a “durable name” which will survive client restarts.
	// Durable subscriptions cause the server to track the last acknowledged message
	// sequence number for a client and durable name. When the client restarts/resubscribes,
	// and uses the same client ID and durable name, the server will resume delivery beginning
	// with the earliest unacknowledged message for this durable subscription.
	//
	// Doing this causes the NATS Streaming server to track
	// the last acknowledged message for that ClientID + DurableName.
	DurableName string

	// SubscribersCount determines wow much concurrent subscribers should be started.
	SubscribersCount int

	// How long subscriber should wait for Ack/Nack. When no Ack/Nack was received, message will be redelivered.
	// It is mapped to stan.AckWait option.
	AckWaitTimeout time.Duration

	// CloseTimeout determines how long subscriber will wait for Ack/Nack on close.
	// When no Ack/Nack is received after CloseTimeout, subscriber will be closed.
	CloseTimeout time.Duration
}

func (c *StreamingSubscriberConfig) GetStreamingSubscriberSubscriptionConfig() StreamingSubscriberSubscriptionConfig {
	return StreamingSubscriberSubscriptionConfig{
		Unmarshaler:      c.Unmarshaler,
		QueueGroup:       c.QueueGroup,
		DurableName:      c.DurableName,
		SubscribersCount: c.SubscribersCount,
		AckWaitTimeout:   c.AckWaitTimeout,
		CloseTimeout:     c.CloseTimeout,
	}
}

func (c *StreamingSubscriberSubscriptionConfig) setDefaults() {
	if c.SubscribersCount <= 0 {
		c.SubscribersCount = 1
	}
	if c.CloseTimeout <= 0 {
		c.CloseTimeout = time.Second * 30
	}
	if c.AckWaitTimeout <= 0 {
		c.AckWaitTimeout = time.Second * 30
	}
}

func (c *StreamingSubscriberSubscriptionConfig) Validate() error {
	if c.Unmarshaler == nil {
		return errors.New("StreamingSubscriberConfig.Unmarshaler is missing")
	}

	if c.QueueGroup == "" && c.SubscribersCount > 1 {
		return errors.New(
			"to set StreamingSubscriberConfig.SubscribersCount " +
				"you need to also set StreamingSubscriberConfig.QueueGroup, " +
				"in other case you will receive duplicated messages",
		)
	}

	return nil
}

type StreamingSubscriber struct {
	conn   nats.Conn
	logger watermill.LoggerAdapter

	config StreamingSubscriberSubscriptionConfig

	subs     []nats.Subscription
	subsLock sync.RWMutex

	closed  bool
	closing chan struct{}

	outputsWg            sync.WaitGroup
	processingMessagesWg sync.WaitGroup
}

// NewStreamingSubscriber creates a new StreamingSubscriber.
//
// When using custom NATS hostname, you should pass it by options StreamingSubscriberConfig.NatsOptions:
//		// ...
//		NatsOptions: []nats.Option{
//			nats.NatsURL("nats://your-nats-hostname:4222"),
//		}
//		// ...
func NewStreamingSubscriber(config StreamingSubscriberConfig, logger watermill.LoggerAdapter) (*StreamingSubscriber, error) {
	conn, err := nats.Connect(config.ClusterID, config.NatsOptions...)
	if err != nil {
		return nil, errors.Wrap(err, "cannot connect to NATS")
	}
	return NewStreamingSubscriberWithNatsConn(*conn, config.GetStreamingSubscriberSubscriptionConfig(), logger)
}

func NewStreamingSubscriberWithNatsConn(conn nats.Conn, config StreamingSubscriberSubscriptionConfig, logger watermill.LoggerAdapter) (*StreamingSubscriber, error) {
	config.setDefaults()

	if err := config.Validate(); err != nil {
		return nil, err
	}

	if logger == nil {
		logger = watermill.NopLogger{}
	}

	return &StreamingSubscriber{
		conn:    conn,
		logger:  logger,
		config:  config,
		closing: make(chan struct{}),
	}, nil
}

// Subscribe subscribes messages from NATS Streaming.
//
// Subscribe will spawn SubscribersCount goroutines making subscribe.
func (s *StreamingSubscriber) Subscribe(ctx context.Context, topic string) (<-chan *message.Message, error) {
	output := make(chan *message.Message)

	for i := 0; i < s.config.SubscribersCount; i++ {
		s.outputsWg.Add(1)
		subscriberLogFields := watermill.LogFields{
			"subscriber_num": i,
			"topic":          topic,
		}

		s.logger.Debug("Starting subscriber", subscriberLogFields)

		processMessagesWg := &sync.WaitGroup{}

		sub, err := s.subscribe(ctx, output, topic, subscriberLogFields, processMessagesWg)
		if err != nil {
			return nil, errors.Wrap(err, "cannot subscribe")
		}

		go func(subscriber nats.Subscription, subscriberLogFields watermill.LogFields) {
			select {
			case <-s.closing:
				// unblock
			case <-ctx.Done():
				// unblock
			}
			// if err := sub.Close(); err != nil {
			// 	s.logger.Error("Cannot close subscriber", err, subscriberLogFields)
			// }

			processMessagesWg.Wait()
			s.outputsWg.Done()
		}(*sub, subscriberLogFields)

		s.subsLock.Lock()
		s.subs = append(s.subs, *sub)
		s.subsLock.Unlock()
	}

	go func() {
		s.outputsWg.Wait()
		close(output)
	}()

	return output, nil
}

func (s *StreamingSubscriber) SubscribeInitialize(topic string) (err error) {
	_, err = s.subscribe(
		context.Background(),
		make(chan *message.Message),
		topic,
		nil,
		&sync.WaitGroup{},
	)
	if err != nil {
		return errors.Wrap(err, "cannot initialize subscribe")
	}

	// return errors.Wrap(sub.Close(), "cannot close after subscribe initialize")
	return nil
}

func (s *StreamingSubscriber) subscribe(
	ctx context.Context,
	output chan *message.Message,
	topic string,
	subscriberLogFields watermill.LogFields,
	processMessagesWg *sync.WaitGroup,
) (*nats.Subscription, error) {
	if s.config.QueueGroup != "" {
		return s.conn.QueueSubscribe(
			topic,
			s.config.QueueGroup,
			func(m *nats.Msg) {
				if s.isClosed() {
					return
				}

				processMessagesWg.Add(1)
				defer processMessagesWg.Done()

				s.processMessage(ctx, m, output, subscriberLogFields)
			},
		)
	}

	return s.conn.Subscribe(
		topic,
		func(m *nats.Msg) {
			processMessagesWg.Add(1)
			defer processMessagesWg.Done()

			s.processMessage(ctx, m, output, subscriberLogFields)
		},
	)
}

func (s *StreamingSubscriber) processMessage(
	ctx context.Context,
	m *nats.Msg,
	output chan *message.Message,
	logFields watermill.LogFields,
) {
	if s.isClosed() {
		return
	}

	s.processingMessagesWg.Add(1)
	defer s.processingMessagesWg.Done()

	s.logger.Trace("Received message", logFields)

	msg, err := s.config.Unmarshaler.Unmarshal(m)
	if err != nil {
		s.logger.Error("Cannot unmarshal message", err, logFields)
		return
	}

	ctx, cancelCtx := context.WithCancel(ctx)
	msg.SetContext(ctx)
	defer cancelCtx()

	messageLogFields := logFields.Add(watermill.LogFields{"message_uuid": msg.UUID})
	s.logger.Trace("Unmarshaled message", messageLogFields)

	select {
	case output <- msg:
		s.logger.Trace("Message sent to consumer", messageLogFields)
	case <-s.closing:
		s.logger.Trace("Closing, message discarded", messageLogFields)
		return
	case <-ctx.Done():
		s.logger.Trace("Context cancelled, message discarded", messageLogFields)
		return
	}

	select {
	case <-msg.Acked():
		if err := m.Ack(); err != nil {
			s.logger.Error("Cannot send ack", err, messageLogFields)
			return
		}
		s.logger.Trace("Message Acked", messageLogFields)
	case <-msg.Nacked():
		s.logger.Trace("Message Nacked", messageLogFields)
		return
	case <-time.After(s.config.AckWaitTimeout):
		s.logger.Trace("Ack timeouted", messageLogFields)
		return
	case <-s.closing:
		s.logger.Trace("Closing, message discarded before ack", messageLogFields)
		return
	case <-ctx.Done():
		s.logger.Trace("Context cancelled, message discarded before ack", messageLogFields)
		return
	}
}

func (s *StreamingSubscriber) Close() error {
	s.subsLock.Lock()
	defer s.subsLock.Unlock()

	if s.closed {
		return nil
	}
	s.closed = true

	s.logger.Debug("Closing subscriber", nil)
	defer s.logger.Info("StreamingSubscriber closed", nil)

	var result error

	close(s.closing)
	internalSync.WaitGroupTimeout(&s.outputsWg, s.config.CloseTimeout)

	s.conn.Close()

	return result
}

func (s *StreamingSubscriber) isClosed() bool {
	s.subsLock.RLock()
	defer s.subsLock.RUnlock()

	return s.closed
}
