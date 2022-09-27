package common

import (
	"encoding/json"
	"log"

	"github.com/Shopify/sarama"
)

type EventProducer struct {
	producer sarama.AsyncProducer
	topic    string
}

func NewEventProducer(topic string) *EventProducer {
	pro := NewAsyncProducer()
	e := &EventProducer{
		topic:    topic,
		producer: pro,
	}
	go e.asyncDealMessage()
	return e
}

func (e *EventProducer) asyncDealMessage() {
	for {
		select {
		case res := <-e.producer.Successes():
			log.Println("push msg success", "topic is", res.Topic, "partition is ", res.Partition, "offset is ", res.Offset)
		case err := <-e.producer.Errors():
			log.Println("push msg failed", "err is ", err.Error())
		}
	}
}

func (e *EventProducer) Producer(data *KafkaMsg) {
	bytes, err := json.Marshal(data)
	if err != nil {
		log.Println("marshal data failed", "err is ", err.Error())
		return
	}
	e.producer.Input() <- &sarama.ProducerMessage{
		Value: sarama.ByteEncoder(bytes),
		Topic: e.topic,
	}
}
