package eventFlow

import (
	"encoding/json"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/google/uuid"
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
	UUID      string    `json:"uuid"`
	Timestamp time.Time `json:"timestamp"`
	EventType EventType `json:"event_type"`
	Triggerer string    `json:"triggerer"`
	Payload   T         `json:"payload"`
}

func NewEvent[T any](triggerer string, eventType EventType, payload T) *Event[T] {
	return &Event[T]{
		UUID:      uuid.NewString(),
		Timestamp: time.Now(),
		EventType: eventType,
		Triggerer: triggerer,
		Payload:   payload,
	}
}

type EventFlow[T any] struct {
	client    *Client
	eventType EventType
	qos       QoS
}

func NewEventFlow[T any](client *Client, eventType EventType, qos QoS) *EventFlow[T] {
	return &EventFlow[T]{
		client:    client,
		eventType: eventType,
		qos:       qos,
	}
}

func (flow *EventFlow[T]) Subscribe(callback func(event Event[T])) error {
	token := flow.client.mqttClient.Subscribe(string(flow.eventType), byte(flow.qos), func(c mqtt.Client, m mqtt.Message) {
		var event Event[T]

		err := json.Unmarshal(m.Payload(), &event)
		if err != nil {
			log.Error().
				Err(err).
				Str("client", flow.client.name).
				Msg("failed to unmarshal event from json")
			return
		}

		log.Info().
			Time("timestamp", event.Timestamp).
			Str("uuid", event.UUID).
			Str("client", flow.client.name).
			Str("event_type", string(flow.eventType)).
			Msg("event flow received an event")

		callback(event)
	})

	token.Wait()

	if token.Error() != nil {
		log.Warn().
			Err(token.Error()).
			Str("client", flow.client.name).
			Str("event_type", string(flow.eventType)).
			Msg("event flow failed to subscribe")
		return token.Error()
	}

	log.Info().
		Str("client", flow.client.name).
		Str("event_type", string(flow.eventType)).
		Msg("event flow subscribed")

	return nil
}

func (flow *EventFlow[T]) Unsubscribe() error {
	token := flow.client.mqttClient.Unsubscribe(string(flow.eventType))

	token.Wait()

	if token.Error() != nil {
		log.Warn().
			Err(token.Error()).
			Str("client", flow.client.name).
			Str("event_type", string(flow.eventType)).
			Msg("event flow failed to unsubscribe")
		return token.Error()
	}

	log.Info().
		Str("client", flow.client.name).
		Str("event_type", string(flow.eventType)).
		Msg("event flow unsubscribed")

	return nil
}

func (flow *EventFlow[T]) Publish(payload T) error {
	event := NewEvent(flow.client.name, flow.eventType, payload)

	bytes, err := json.Marshal(event)
	if err != nil {
		log.Error().
			Err(err).
			Time("timestamp", event.Timestamp).
			Str("uuid", event.UUID).
			Str("client", flow.client.name).
			Msg("failed to marshal the event to json")
		return err
	}

	token := flow.client.mqttClient.Publish(string(flow.eventType), byte(flow.qos), false, bytes)

	token.Wait()

	if token.Error() != nil {
		log.Warn().
			Err(token.Error()).
			Time("timestamp", event.Timestamp).
			Str("uuid", event.UUID).
			Str("client", flow.client.name).
			Str("event_type", string(flow.eventType)).
			Msg("event flow failed to publish")
		return token.Error()
	}

	log.Info().
		Time("timestamp", event.Timestamp).
		Str("uuid", event.UUID).
		Str("client", flow.client.name).
		Str("event_type", string(flow.eventType)).
		Msg("event flow published")

	return nil
}
