package jetstream

import (
	nats "github.com/nats-io/nats.go"
	"github.com/pkg/errors"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

type StreamingPublisherConfig struct {
	// URL is the NATS URL.
	URL string

	// NatsOptions are custom options for a connection.
	NatsOptions []nats.Option

	// Marshaler is marshaler used to marshal messages to stan format.
	Marshaler Marshaler
}

type StreamingPublisherPublishConfig struct {
	// Marshaler is marshaler used to marshal messages to stan format.
	Marshaler Marshaler
}

func (c StreamingPublisherConfig) Validate() error {
	if c.Marshaler == nil {
		return errors.New("StreamingPublisherConfig.Marshaler is missing")
	}

	return nil
}

func (c StreamingPublisherConfig) GetStreamingPublisherPublishConfig() StreamingPublisherPublishConfig {
	return StreamingPublisherPublishConfig{
		Marshaler: c.Marshaler,
	}
}

type StreamingPublisher struct {
	conn   nats.Conn
	config StreamingPublisherPublishConfig
	logger watermill.LoggerAdapter
}

// NewNatsStreamingPublisher creates a new StreamingPublisher.
//
// When using custom NATS hostname, you should pass it by options StreamingPublisherConfig.StanOptions:
//		// ...
//		NatsOptions: []nats.Option{
//			nats.NatsURL("nats://your-nats-hostname:4222"),
//		}
//		// ...
func NewNatsStreamingPublisher(config StreamingPublisherConfig, logger watermill.LoggerAdapter) (*StreamingPublisher, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}

	conn, err := nats.Connect(config.URL, config.NatsOptions...)
	if err != nil {
		return nil, errors.Wrap(err, "cannot connect to nats")
	}

	return NewNatsStreamingPublisherWithNatsConn(*conn, config.GetStreamingPublisherPublishConfig(), logger)
}

func NewNatsStreamingPublisherWithNatsConn(conn nats.Conn, config StreamingPublisherPublishConfig, logger watermill.LoggerAdapter) (*StreamingPublisher, error) {
	if logger == nil {
		logger = watermill.NopLogger{}
	}

	return &StreamingPublisher{
		conn:   conn,
		config: config,
		logger: logger,
	}, nil
}

// Publish publishes message to NATS.
//
// Publish will not return until an ack has been received from NATS Streaming.
// When one of messages delivery fails - function is interrupted.
func (p StreamingPublisher) Publish(topic string, messages ...*message.Message) error {
	for _, msg := range messages {
		messageFields := watermill.LogFields{
			"message_uuid": msg.UUID,
			"topic_name":   topic,
		}

		p.logger.Trace("Publishing message", messageFields)

		b, err := p.config.Marshaler.Marshal(topic, msg)
		if err != nil {
			return err
		}

		if err := p.conn.Publish(topic, b); err != nil {
			return errors.Wrap(err, "sending message failed")
		}
	}

	return nil
}

func (p StreamingPublisher) Close() error {
	p.logger.Trace("Closing publisher", nil)
	defer p.logger.Trace("StreamingPublisher closed", nil)

	p.conn.Close()

	return nil
}
