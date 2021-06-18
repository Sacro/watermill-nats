package jetstream_test

import (
	"os"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-jetstream/pkg/jetstream"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/tests"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"
)

func newPubSub(t *testing.T, clientID string, queueName string) (message.Publisher, message.Subscriber) {
	logger := watermill.NewStdLogger(true, true)

	natsURL := os.Getenv("WATERMILL_TEST_NATS_URL")
	if natsURL == "" {
		natsURL = nats.DefaultURL
	}

	options := []nats.Option{}

	pub, err := jetstream.NewNatsStreamingPublisher(jetstream.StreamingPublisherConfig{
		URL:   natsURL,
		Marshaler:   jetstream.GobMarshaler{},
		NatsOptions: options,
	}, logger)
	require.NoError(t, err)

	sub, err := jetstream.NewStreamingSubscriber(jetstream.StreamingSubscriberConfig{
		ClusterID:        natsURL,
		ClientID:         clientID + "_sub",
		QueueGroup:       queueName,
		DurableName:      "durable-name",
		SubscribersCount: 10,
		AckWaitTimeout:   time.Second, // AckTiemout < 5 required for continueAfterErrors
		Unmarshaler:      jetstream.GobMarshaler{},
		NatsOptions:      options,
	}, logger)
	require.NoError(t, err)

	return pub, sub
}

func createPubSub(t *testing.T) (message.Publisher, message.Subscriber) {
	return newPubSub(t, watermill.NewUUID(), "test-queue")
}

func createPubSubWithDurable(t *testing.T, consumerGroup string) (message.Publisher, message.Subscriber) {
	return newPubSub(t, consumerGroup, consumerGroup)
}

func TestPublishSubscribe(t *testing.T) {
	tests.TestPubSub(
		t,
		tests.Features{
			ConsumerGroups:      false,
			ExactlyOnceDelivery: false,
			GuaranteedOrder:     false,
			Persistent:          true,
		},
		createPubSub,
		createPubSubWithDurable,
	)
}
