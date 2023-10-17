package async

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/pubsub"
	vkit "cloud.google.com/go/pubsub/apiv1"
	"github.com/0nramp/utils/xlog"
	"github.com/googleapis/gax-go/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

var config = &pubsub.ClientConfig{
	PublisherCallOptions: &vkit.PublisherCallOptions{
		Publish: []gax.CallOption{
			gax.WithRetry(func() gax.Retryer {
				return gax.OnCodes([]codes.Code{
					codes.Aborted,
					codes.Canceled,
					codes.Internal,
					codes.ResourceExhausted,
					codes.Unknown,
					codes.Unavailable,
					codes.DeadlineExceeded,
				}, gax.Backoff{
					Initial:    50 * time.Millisecond,
					Max:        500 * time.Millisecond,
					Multiplier: 1.3,
				})
			}),
		},
	},
}

type Publisher interface {
	Publish(ctx context.Context, msg []byte) (string, error)
	Close() error
}

type PubSubPublisher struct {
	*pubsub.Client
	topicID string
}

func (p *PubSubPublisher) Publish(ctx context.Context, msg []byte) (string, error) {
	t := p.Client.Topic(p.topicID)

	exists, err := t.Exists(ctx)
	if err != nil {
		return "", fmt.Errorf("error looking for topic with id [%s]: %w", p.topicID, err)
	}

	if !exists {
		return "", fmt.Errorf("topic does not exist: [%s]", p.topicID)
	}

	result := t.Publish(ctx, &pubsub.Message{
		Data: msg,
	})
	// Block until the result is returned and a server-generated ID
	// is returned for the published message.
	// TODO: configure to mitigate against this causing a timeout
	// in the flow where publisher is called,
	// as well as against quiet undetected publishing failures
	// https://cloud.google.com/pubsub/docs/publisher#flow_control
	id, err := result.Get(ctx)
	if err != nil {
		return "", fmt.Errorf("pubsub: result.Get: %w", err)
	}
	return id, nil
}

func (c *PubSubPublisher) Close() error {
	return c.Client.Close()
}

func NewPubSubPublisher(ctx context.Context, projectID string, topicID string) (*PubSubPublisher, error) {
	localConfig(projectID)

	c, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		return nil, fmt.Errorf("pubsub: NewClient: %w", err)
	}

	return &PubSubPublisher{c, topicID}, nil
}

// When failing on publish is not an option, this publisher will try harder.
// With hardcoded settings it'll try 7-8 times in .5 seconds backing off exponentially, then fail
// This is set with an eye toward publishing in the context of an NBS call to avoid timeouts.
func NewPubSubPublisherWithRetries(ctx context.Context, projectID string, topicID string) (*PubSubPublisher, error) {
	localConfig(projectID)

	c, err := pubsub.NewClientWithConfig(ctx, projectID, config)
	if err != nil {
		return nil, fmt.Errorf("pubsub: NewClient: %w", err)
	}

	return &PubSubPublisher{c, topicID}, nil
}

type PubSubProtoPublisher[T any] struct {
	p         Publisher
	translate func(d T) (proto.Message, error)
}

func (p *PubSubProtoPublisher[T]) Publish(ctx context.Context, msg T) (string, error) {
	ctxLog := xlog.WithContext(ctx)
	pb, err := p.translate(msg)
	if err != nil {
		return "", fmt.Errorf("pubsub: could not translate %v: %w", msg, err)
	}

	// encode messages as json for better dev experience for now
	m, err := protojson.Marshal(pb)
	if err != nil {
		return "", fmt.Errorf("protojson.Marshal err: %w", err)
	}

	ctxLog.Infof("PUBLISHING: %s\n\n", string(m))

	return p.p.Publish(ctx, m)
}

func (p *PubSubProtoPublisher[T]) Close() error {
	return p.p.Close()
}

func NewPubSubProtoPublisher[T any](ctx context.Context, projectID string, topicID string, translate func(d T) (proto.Message, error)) (*PubSubProtoPublisher[T], error) {
	p, err := NewPubSubPublisher(ctx, projectID, topicID)
	if err != nil {
		return nil, fmt.Errorf("pubsub: NewClient: %w", err)
	}

	return &PubSubProtoPublisher[T]{p, translate}, nil
}

func NewFromPublisher[T any](p Publisher, translate func(d T) (proto.Message, error)) *PubSubProtoPublisher[T] {
	return &PubSubProtoPublisher[T]{p, translate}
}
