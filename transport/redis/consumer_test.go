//go:build test

package redis

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/pokt-network/pocket-relay-miner/transport"
)

// buildPooledTestMessage serializes a MinedRelayMessage into a Redis
// XMessage the way the publisher does, so parseMessage can exercise the
// real unmarshal + pool path.
func buildPooledTestMessage(t *testing.T, sessionID, serviceID string, cu uint64) redis.XMessage {
	t.Helper()
	relay := &transport.MinedRelayMessage{
		RelayHash:               []byte{0x01, 0x02, 0x03, 0x04},
		RelayBytes:              []byte("relay-payload-bytes"),
		ComputeUnitsPerRelay:    cu,
		SessionId:               sessionID,
		SessionEndHeight:        200,
		SupplierOperatorAddress: "pokt1supplier",
		ServiceId:               serviceID,
		ApplicationAddress:      "pokt1app",
		ArrivalBlockHeight:      150,
		PublishedAtUnixNano:     1776206000,
		SessionStartHeight:      100,
	}
	buf, err := relay.Marshal()
	require.NoError(t, err)
	return redis.XMessage{
		ID:     "1-0",
		Values: map[string]interface{}{"data": string(buf)},
	}
}

// newTestConsumer returns a StreamsConsumer stub that has just enough
// state to call parseMessage directly. The unit under test is the
// parseMessage method itself, not the Consume loop.
func newTestConsumer() *StreamsConsumer {
	return &StreamsConsumer{streamName: "ha:relays:pokt1test"}
}

// TestParseMessage_ReturnsPooledMessage verifies parseMessage pulls a
// MinedRelayMessage from the transport pool and returns a correctly
// populated StreamMessage. Releasing it back to the pool must scrub
// every field so a subsequent Acquire sees no stale data.
func TestParseMessage_ReturnsPooledMessage(t *testing.T) {
	c := newTestConsumer()
	xmsg := buildPooledTestMessage(t, "sess-1", "develop-http", 1000)

	msg, err := c.parseMessage(xmsg, "ha:relays:pokt1test")
	require.NoError(t, err)
	require.NotNil(t, msg.Message)

	assert.Equal(t, "sess-1", msg.Message.SessionId)
	assert.Equal(t, "develop-http", msg.Message.ServiceId)
	assert.Equal(t, uint64(1000), msg.Message.ComputeUnitsPerRelay)
	assert.Equal(t, []byte("relay-payload-bytes"), msg.Message.RelayBytes)

	// Release and confirm scrub.
	released := msg.Message
	transport.ReleaseMinedRelayMessage(released)
	assert.Empty(t, released.SessionId)
	assert.Empty(t, released.ServiceId)
	assert.Zero(t, released.ComputeUnitsPerRelay)
	assert.Empty(t, released.RelayBytes)
}

// TestParseMessage_NoCrossRelayLeak writes two different relays through
// parseMessage+Release sequentially and proves the second never sees
// fields from the first. This is the end-to-end correctness contract
// of the pool on the real hot path.
func TestParseMessage_NoCrossRelayLeak(t *testing.T) {
	c := newTestConsumer()

	first := buildPooledTestMessage(t, "sess-first", "svc-A", 500)
	msg1, err := c.parseMessage(first, "ha:relays:pokt1test")
	require.NoError(t, err)
	assert.Equal(t, "sess-first", msg1.Message.SessionId)
	assert.Equal(t, "svc-A", msg1.Message.ServiceId)
	transport.ReleaseMinedRelayMessage(msg1.Message)

	second := buildPooledTestMessage(t, "sess-second", "svc-B", 2000)
	msg2, err := c.parseMessage(second, "ha:relays:pokt1test")
	require.NoError(t, err)
	require.NotNil(t, msg2.Message)
	assert.Equal(t, "sess-second", msg2.Message.SessionId)
	assert.Equal(t, "svc-B", msg2.Message.ServiceId)
	assert.Equal(t, uint64(2000), msg2.Message.ComputeUnitsPerRelay)
	transport.ReleaseMinedRelayMessage(msg2.Message)
}

// TestParseMessage_InvalidPayloadReleasesPooledMessage guards the error
// path: when Unmarshal fails, parseMessage must still return the pooled
// message to avoid draining the pool over time.
func TestParseMessage_InvalidPayloadReleasesPooledMessage(t *testing.T) {
	c := newTestConsumer()
	bad := redis.XMessage{
		ID:     "bad-0",
		Values: map[string]interface{}{"data": string([]byte{0xff, 0xff, 0xff, 0xff})},
	}

	msg, err := c.parseMessage(bad, "ha:relays:pokt1test")
	require.Error(t, err)
	assert.Nil(t, msg.Message)

	// Pool must still produce a clean message afterwards — if the error
	// path leaked a dirty object, this would fail.
	clean := transport.AcquireMinedRelayMessage()
	assert.Empty(t, clean.SessionId)
	assert.Empty(t, clean.RelayBytes)
	transport.ReleaseMinedRelayMessage(clean)
}

func TestStreamsConsumerCloseUnblocksIdleRead(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	client := redis.NewClient(&redis.Options{
		Addr:     mr.Addr(),
		PoolSize: 4,
	})
	defer func() { require.NoError(t, client.Close()) }()

	consumer, err := NewStreamsConsumer(
		zerolog.Nop(),
		client,
		transport.ConsumerConfig{
			StreamPrefix:            "ha:relays",
			SupplierOperatorAddress: "pokt1shutdown",
			ConsumerGroup:           "ha-miners",
			ConsumerName:            "miner-1",
			BatchSize:               1,
			ClaimIdleTimeout:        30_000,
		},
		0,
	)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	_ = consumer.Consume(ctx)

	// Wait until the stream/group exists so the consumer has reached the
	// blocking XREADGROUP path rather than merely racing startup.
	require.Eventually(t, func() bool {
		return mr.Exists("ha:relays:pokt1shutdown")
	}, time.Second, 10*time.Millisecond)

	cancel()

	done := make(chan error, 1)
	go func() {
		done <- consumer.Close()
	}()

	select {
	case closeErr := <-done:
		require.NoError(t, closeErr)
	case <-time.After(3 * streamsReadBlockTimeout):
		t.Fatal("StreamsConsumer.Close remained blocked after bounded XREADGROUP cancellation")
	}
}

func TestStreamsConsumerBoundedBlockStillDeliversRelay(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer func() { require.NoError(t, client.Close()) }()

	consumer, err := NewStreamsConsumer(
		zerolog.Nop(),
		client,
		transport.ConsumerConfig{
			StreamPrefix:            "ha:relays",
			SupplierOperatorAddress: "pokt1push",
			ConsumerGroup:           "ha-miners",
			ConsumerName:            "miner-1",
			BatchSize:               1,
			ClaimIdleTimeout:        30_000,
		},
		0,
	)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	msgCh := consumer.Consume(ctx)
	defer func() { require.NoError(t, consumer.Close()) }()

	require.Eventually(t, func() bool {
		return mr.Exists("ha:relays:pokt1push")
	}, time.Second, 10*time.Millisecond)

	relay := newTestMinedRelayMessage("pokt1push", "session-push")
	payload, err := relay.Marshal()
	require.NoError(t, err)

	_, err = client.XAdd(ctx, &redis.XAddArgs{
		Stream: "ha:relays:pokt1push",
		Values: map[string]interface{}{"data": string(payload)},
	}).Result()
	require.NoError(t, err)

	select {
	case msg := <-msgCh:
		require.NotNil(t, msg.Message)
		assert.Equal(t, "session-push", msg.Message.SessionId)
		transport.ReleaseMinedRelayMessage(msg.Message)
	case <-time.After(2 * streamsReadBlockTimeout):
		t.Fatal("relay was not delivered through bounded XREADGROUP")
	}
}

func TestAckMessageFallsBackToAckAndDelete(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	ctx := context.Background()
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer func() { require.NoError(t, client.Close()) }()

	streamName := "ha:relays:pokt1supplier"
	groupName := "ha-miners"
	messageID, err := client.XAdd(ctx, &redis.XAddArgs{
		Stream: streamName,
		Values: map[string]interface{}{"data": "payload"},
	}).Result()
	require.NoError(t, err)
	require.NoError(t, client.XGroupCreate(ctx, streamName, groupName, "0").Err())

	streams, err := client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    groupName,
		Consumer: "consumer-1",
		Streams:  []string{streamName, ">"},
		Count:    1,
	}).Result()
	require.NoError(t, err)
	require.Len(t, streams, 1)
	require.Len(t, streams[0].Messages, 1)
	require.Equal(t, messageID, streams[0].Messages[0].ID)

	consumer := &StreamsConsumer{
		logger: zerolog.Nop(),
		client: client,
		config: transport.ConsumerConfig{
			ConsumerGroup: groupName,
		},
	}

	require.NoError(t, consumer.AckMessage(ctx, transport.StreamMessage{
		ID:         messageID,
		StreamName: streamName,
	}))

	xlen, err := client.XLen(ctx, streamName).Result()
	require.NoError(t, err)
	require.Zero(t, xlen, "ack+delete must remove processed stream entries")
}

func TestPublisherRefreshesStreamTTLRateLimited(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	ctx := context.Background()
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer func() { require.NoError(t, client.Close()) }()

	publisher := NewStreamsPublisher(zerolog.Nop(), client, "ha:relays", 4*time.Minute)
	msg := newTestMinedRelayMessage("pokt1supplier", "session-1")
	require.NoError(t, publisher.Publish(ctx, msg))

	streamName := "ha:relays:pokt1supplier"
	initialTTL := mr.TTL(streamName)
	require.Greater(t, initialTTL, 3*time.Minute)

	mr.FastForward(2 * time.Minute)
	publisher.ttlLastRefresh.Store(streamName, time.Now().Add(-2*time.Minute))
	msg = newTestMinedRelayMessage("pokt1supplier", "session-1")
	require.NoError(t, publisher.Publish(ctx, msg))

	refreshedTTL := mr.TTL(streamName)
	require.Greater(t, refreshedTTL, 3*time.Minute, "TTL should refresh after the rate limit interval")
}

func newTestMinedRelayMessage(supplier, sessionID string) *transport.MinedRelayMessage {
	return &transport.MinedRelayMessage{
		RelayHash:               []byte{0x01, 0x02, 0x03, 0x04},
		RelayBytes:              []byte(fmt.Sprintf("relay-%s", sessionID)),
		ComputeUnitsPerRelay:    100,
		SessionId:               sessionID,
		SessionEndHeight:        200,
		SupplierOperatorAddress: supplier,
		ServiceId:               "svc-A",
		ApplicationAddress:      "pokt1app",
		ArrivalBlockHeight:      150,
		SessionStartHeight:      100,
	}
}
