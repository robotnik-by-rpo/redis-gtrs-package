package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/dranikpg/gtrs"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

type EventInterface interface{
	ToString() string
}

type Event struct{
	URL string
	Target string `gtrs:"to"`
}


type RedisStream struct{
	log *zap.Logger
	stream *gtrs.Stream[EventInterface]
	consumer *gtrs.StreamConsumer[EventInterface]
}

// InitEvent function for initing redis stream
func InitEvent( log *zap.Logger,
				redisClient *redis.Client,
				ctx context.Context,
				streamName string) *RedisStream{
	const op = "redis.redis.InitEvent"

	log.Info(fmt.Sprintf("%s: initing new stream", op))
	stream := gtrs.NewStream[EventInterface](redisClient, streamName, &gtrs.Options{
		TTL:    time.Hour,
		MaxLen: 1000,
		Approx: true,
	})

	log.Info(fmt.Sprintf("%s: returning RedisStream", op))
	return &RedisStream{
		log:    log,
		stream: &stream,
		consumer: gtrs.NewConsumer[EventInterface](ctx, redisClient, gtrs.StreamIDs{streamName:"0"}),
		
	}
}
// AddEvent method for adding event into queue
func (r RedisStream) AddEvent(ctx context.Context, event EventInterface) error{
	const op = "redis.redis.AddEvent"
	r.log.Info(fmt.Sprintf("%s: adding new event",op))
	if _, err := r.stream.Add(ctx, event); err != nil{
		return fmt.Errorf("%s: %v",op, err)
	}

	return nil
}

// ReadEvent method for reading event from queue
func (r RedisStream) ReadEvent( ctx context.Context, 
								streamName string) *EventInterface{

	const op = "redis.redis.ReadEvent"
	r.log.Info(fmt.Sprintf("%s: initing new consumer for %s",op,streamName))
	defer r.Close(streamName)
	for msg := range r.consumer.Chan(){
		if msg.Err == nil{
			r.log.Info(fmt.Sprintf("%s: getting event from %s",op,streamName))
			event := msg.Data
			return &event	
		}
		switch msg.Err.(type){
		case gtrs.ReadError:
			r.log.Error(fmt.Sprintf("%s: %v",op,msg.Err))
			return nil
		case gtrs.AckError:
			r.log.Error(fmt.Sprintf("%s: %v",op,msg.Err))
			return nil
		case gtrs.ParseError:
			r.log.Error(fmt.Sprintf("%s: %v",op,msg.Err))
			return nil
		}
		
	}
	return nil		
}

// Close method for closing connection
func (r RedisStream) Close(streamName string){
	const op = "redis.redis.Close"
	r.log.Info(fmt.Sprintf("%s: closing consumer for %s",op,streamName)) 
	r.consumer.Close()
}