package eventFlow

import (
	"encoding/json"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/rs/zerolog/log"
)

type QoS byte

const (
	AtMostOnce  QoS = 0
	AtLeastOnce QoS = 1
	ExactlyOnce QoS = 2
)

type EventType string

type Event[T any] struct {
	Timestamp time.Time `json:"timestamp"`
	EventType EventType `json:"event_type"`
	Payload   T         `json:"payload"`
}

func NewEvent[T any](eventType EventType, payload T) *Event[T] {
	return &Event[T]{
		Timestamp: time.Now(),
		EventType: eventType,
		Payload:   payload,
	}
}

type EventListener[T any] struct {
	client    *Client
	eventType EventType
	qos       QoS
}

func NewEventListener[T any](client *Client, eventType EventType, qos QoS) *EventListener[T] {
	return &EventListener[T]{
		client:    client,
		eventType: eventType,
		qos:       qos,
	}
}

func (listener *EventListener[T]) Subscribe() (chan Event[T], error) {
	channel := make(chan Event[T])

	token := listener.client.mqttClient.Subscribe(string(listener.eventType), byte(listener.qos), func(c mqtt.Client, m mqtt.Message) {
		log.Info().
			Str("topic", string(listener.eventType)).
			Msg("Event listener received an event")

		var event Event[T]

		err := json.Unmarshal(m.Payload(), &event)
		if err != nil {
			log.Error().Err(err).Msg("Failed to unmarshal event from json")
			return
		}

		channel <- event
	})

	token.Wait()

	if token.Error() != nil {
		log.Warn().
			Err(token.Error()).
			Str("topic", string(listener.eventType)).
			Msg("Event listener failed to subscribe")
		return nil, token.Error()
	}

	log.Info().
		Str("topic", string(listener.eventType)).
		Msg("Event listener subscribed")

	return channel, nil
}

func (listener *EventListener[T]) Unsubscribe() error {
	token := listener.client.mqttClient.Unsubscribe(string(listener.eventType))

	token.Wait()

	if token.Error() != nil {
		log.Warn().
			Err(token.Error()).
			Str("topic", string(listener.eventType)).
			Msg("Event listener failed to unsubscribe")
		return token.Error()
	}

	log.Info().
		Str("topic", string(listener.eventType)).
		Msg("Event listener unsubscribed")

	return nil
}

func (listener *EventListener[T]) Publish(payload T) error {
	event := NewEvent(listener.eventType, payload)

	bytes, err := json.Marshal(event)
	if err != nil {
		log.Error().Err(err).Msg("Failed to marshal the event to json")
		return err
	}

	token := listener.client.mqttClient.Publish(string(listener.eventType), byte(listener.qos), false, bytes)

	token.Wait()

	if token.Error() != nil {
		log.Warn().
			Err(token.Error()).
			Str("topic", string(listener.eventType)).
			Msg("Event listener failed to publish")
		return token.Error()
	}

	log.Info().
		Str("topic", string(listener.eventType)).
		Msg("Event listener published")

	return nil
}
