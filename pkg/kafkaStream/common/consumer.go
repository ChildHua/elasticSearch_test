package common

import (
	"context"
	"encoding/json"
	"errors"
	"log"

	"github.com/Shopify/sarama"
)

type EventConsumer struct {
	handler sarama.ConsumerGroupHandler
	group   sarama.ConsumerGroup
	topics  []string
	ctx     context.Context
	cancel  context.CancelFunc
}

func NewConsumer(group string, topics []string, handler *EventHandler) *EventConsumer {
	gp := NewConsumerGroup(group)
	ctx, cancel := context.WithCancel(context.Background())
	return &EventConsumer{
		handler: handler,
		group:   gp,
		topics:  topics,
		ctx:     ctx,
		cancel:  cancel,
	}
}

func (e *EventConsumer) Consume() {
	for {
		select {
		case <-e.ctx.Done():
			e.group.Close()
			log.Println("EventConsumer ctx done")
			return
		default:
			if err := e.group.Consume(e.ctx, e.topics, e.handler); err != nil {
				log.Println("EventConsumer Consume failed err is ", err.Error())
			}
		}
	}
}

func (e *EventConsumer) Stop() {
	e.cancel()
}

type EventHandler struct {
	Claim func(data KafkaMsg) error
}

func (e EventHandler) Setup(session sarama.ConsumerGroupSession) error {
	return nil
}

func (e EventHandler) Cleanup(session sarama.ConsumerGroupSession) error {
	return nil
}

func (e EventHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		var data KafkaMsg
		// var data map[string]interface{}
		if err := json.Unmarshal(msg.Value, &data); err != nil {
			return errors.New("failed to unmarshal message err is " + err.Error())
		}
		err := e.Claim(data)
		if err != nil {
			return errors.New("failed to claim:" + err.Error())
		}
		// 处理消息成功后标记为处理, 然后会自动提交
		session.MarkMessage(msg, "")
	}
	return nil
}
