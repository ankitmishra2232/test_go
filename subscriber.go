package async

import (
	"context"
	"fmt"

	"cloud.google.com/go/pubsub"
	"github.com/0nramp/utils/xlog"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type PubSubSubscriber struct {
	*pubsub.Client
	subscriptionID string
}

func (s *PubSubSubscriber) Subscribe(ctx context.Context, handler func(ctx context.Context, data []byte) error) error {
	ctxLog := xlog.WithContext(ctx)
	sub := s.Client.Subscription(s.subscriptionID)
	exists, err := sub.Exists(ctx)
	if err != nil {
		return fmt.Errorf("error looking for subscription with id [%s] %w", s.subscriptionID, err)
	}
	if !exists {
		return fmt.Errorf("subscription does not exist: [%s]", s.subscriptionID)
	}

	return sub.Receive(ctx, func(ctx context.Context, message *pubsub.Message) {
		ctxLog.Infof("MESSAGE RECD: %s", message.Data)

		err := handler(ctx, message.Data)
		if err == nil {
			message.Ack()
			return
		}
		ctxLog.Infof("MESSAGE %s NOT ACKED, %w", message.ID, err)
		message.Nack()
	})
}

func (s *PubSubSubscriber) Close() error {
	return s.Client.Close()
}

func NewPubSubSubscriber(ctx context.Context, projectID string, subscriptionID string) (*PubSubSubscriber, error) {
	localConfig(projectID)

	c, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		return nil, fmt.Errorf("pubsub: NewClient: %w", err)
	}

	return &PubSubSubscriber{c, subscriptionID}, nil
}

type PubSubProtoSubscriber[T proto.Message] struct {
	s *PubSubSubscriber
	m T
}

func (s *PubSubProtoSubscriber[T]) Subscribe(ctx context.Context, handler func(ctx context.Context, data T) error) {
	hh := func(ctx context.Context, data []byte) error {
		mm := proto.Clone(s.m)
		err := protojson.Unmarshal(data, mm)
		if err != nil {
			return err
		}

		return handler(ctx, mm.(T))
	}

	s.s.Subscribe(ctx, hh)
}

func (s *PubSubProtoSubscriber[T]) Close() error {
	return s.s.Close()
}

func NewPubSubProtoSubscriber[T proto.Message](ctx context.Context, projectID string, subscriptionID string, m T) (*PubSubProtoSubscriber[T], error) {
	s, err := NewPubSubSubscriber(ctx, projectID, subscriptionID)
	if err != nil {
		return nil, err
	}

	return &PubSubProtoSubscriber[T]{s, m}, nil

}
