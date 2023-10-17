package async

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"cloud.google.com/go/pubsub"
)

func localConfig(projectID string) {
	if projectID == "local" {
		if emulatorHost := os.Getenv("PUBSUB_EMULATOR_HOST"); emulatorHost == "" {
			os.Setenv("PUBSUB_EMULATOR_HOST", "pubsub-emulator:8432")
		}
	}

	if projectID == "test" {
		os.Setenv("PUBSUB_EMULATOR_HOST", "localhost:8432")
	}
}

// best effort to create-unless-already-exists for a topic
// even though we check before creating, a race condition could still cause a duplicate/error
func TopicIfNone(ctx context.Context, projectID string, topicID string) (*pubsub.Topic, error) {
	localConfig(projectID)
	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		return nil, fmt.Errorf("failed to get pubsub client: %w", err)
	}
	defer client.Close()

	t := client.Topic(topicID)
	exists, err := t.Exists(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to lookup topic with id [%s] %w", topicID, err)
	}

	if exists {
		return t, nil
	}

	t, err = client.CreateTopic(ctx, topicID)
	if err != nil {
		return nil, fmt.Errorf("CreateTopic: %w", err)
	}
	return t, nil
}

// best effort to create-unless-already-exists for a subscription
// even though we check before creating, a race condition could still cause a duplicate/error
func SubscriptionIfNone(ctx context.Context, projectID string, subscriptionID string, topicID string, dlTopic string) (*pubsub.Subscription, error) {
	localConfig(projectID)
	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		return nil, fmt.Errorf("failed to get pubsub client: %w", err)
	}
	defer client.Close()

	topic := client.Topic(topicID)
	exists, err := topic.Exists(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to lookup topic with id [%s] %w", topicID, err)
	}
	if !exists {
		return nil, fmt.Errorf("expected the topic [%s] to exist (created by the publisher)", topicID)
	}

	s := client.Subscription(subscriptionID)
	exists, err = s.Exists(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to lookup subscription with id [%s] %w", subscriptionID, err)
	}

	if exists {
		return s, nil
	}

	// dead letter topic is per subscription, so we create it if not already there
	// dlTopic is formatted like `projects/my-project/topics/my-dead-letter-topic`
	ss := strings.Split(dlTopic, "/")
	TopicIfNone(ctx, projectID, ss[3])

	// https://onrampcard.atlassian.net/wiki/spaces/EN/pages/165019651/Inter-service+pubsub
	s, err = client.CreateSubscription(ctx, subscriptionID, pubsub.SubscriptionConfig{
		Topic:       topic,
		AckDeadline: 15 * time.Second,
		DeadLetterPolicy: &pubsub.DeadLetterPolicy{
			DeadLetterTopic:     dlTopic,
			MaxDeliveryAttempts: 15,
		},
		RetryPolicy: &pubsub.RetryPolicy{
			MaximumBackoff: 300 * time.Second,
			MinimumBackoff: 5 * time.Second,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create subscription: %w", err)
	}
	return s, nil
}
