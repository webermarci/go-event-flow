package eventFlow

import (
	"encoding/json"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/rs/zerolog/log"
	"github.com/segmentio/ksuid"
)

type QoS byte

const (
	AtMostOnce  QoS = 0
	AtLeastOnce QoS = 1
	ExactlyOnce QoS = 2
)

type EventType string

type Event[T any] struct {
	ID        string    `json:"id"`
	Timestamp time.Time `json:"timestamp"`
	EventType EventType `json:"event_type"`
	Error     error     `json:"error"`
	Triggerer string    `json:"triggerer"`
	Payload   T         `json:"payload"`
}

func NewEvent[T any](triggerer string, eventType EventType, payload T, err error) *Event[T] {
	return &Event[T]{
		ID:        ksuid.New().String(),
		Timestamp: time.Now(),
		EventType: eventType,
		Error:     err,
		Triggerer: triggerer,
		Payload:   payload,
	}
}

func (e *Event[T]) IsSuccessful() bool {
	return e.Error == nil
}

func (e *Event[T]) errorString() string {
	if e.Error == nil {
		return ""
	}
	return e.Error.Error()
}

type EventFlow[T any] struct {
	client    *Client
	EventType EventType
	QoS       QoS
	callback  func(event Event[T])
}

func NewEventFlow[T any](client *Client, eventType EventType, qos QoS) *EventFlow[T] {
	return &EventFlow[T]{
		client:    client,
		EventType: eventType,
		QoS:       qos,
		callback:  func(event Event[T]) {},
	}
}

func (flow *EventFlow[T]) SetCallback(callback func(event Event[T])) {
	flow.callback = callback
}

func (flow *EventFlow[T]) Subscribe() error {
	token := flow.client.mqttClient.Subscribe(string(flow.EventType), byte(flow.QoS), func(c mqtt.Client, m mqtt.Message) {
		var event Event[T]

		err := json.Unmarshal(m.Payload(), &event)
		if err != nil {
			log.Error().
				Err(err).
				Str("triggerer", event.Triggerer).
				Msg("failed to unmarshal event from json")
			return
		}

		log.Info().
			Time("timestamp", event.Timestamp).
			Str("id", event.ID).
			Str("triggerer", event.Triggerer).
			Str("event_type", string(flow.EventType)).
			Msg("event flow received an event")

		flow.callback(event)
	})

	token.Wait()

	if token.Error() != nil {
		log.Warn().
			Err(token.Error()).
			Str("event_type", string(flow.EventType)).
			Msg("event flow failed to subscribe")
		return token.Error()
	}

	log.Info().
		Str("event_type", string(flow.EventType)).
		Msg("event flow subscribed")

	return nil
}

func (flow *EventFlow[T]) Unsubscribe() error {
	token := flow.client.mqttClient.Unsubscribe(string(flow.EventType))

	token.Wait()

	if token.Error() != nil {
		log.Warn().
			Err(token.Error()).
			Str("event_type", string(flow.EventType)).
			Msg("event flow failed to unsubscribe")
		return token.Error()
	}

	log.Info().
		Str("event_type", string(flow.EventType)).
		Msg("event flow unsubscribed")

	return nil
}

func (flow *EventFlow[T]) Publish(triggerer string, payload T, err error) error {
	event := NewEvent(triggerer, flow.EventType, payload, err)

	bytes, err := json.Marshal(event)
	if err != nil {
		log.Error().
			Err(err).
			Time("timestamp", event.Timestamp).
			Str("id", event.ID).
			Str("triggerer", triggerer).
			Msg("failed to marshal the event to json")
		return err
	}

	token := flow.client.mqttClient.Publish(string(flow.EventType), byte(flow.QoS), false, bytes)

	token.Wait()

	if token.Error() != nil {
		log.Warn().
			Err(token.Error()).
			Time("timestamp", event.Timestamp).
			Str("id", event.ID).
			Str("triggerer", triggerer).
			Str("event_type", string(flow.EventType)).
			Str("error", event.errorString()).
			Msg("event flow failed to publish an event")
		return token.Error()
	}

	log.Info().
		Time("timestamp", event.Timestamp).
		Str("id", event.ID).
		Str("triggerer", triggerer).
		Str("event_type", string(flow.EventType)).
		Str("error", event.errorString()).
		Msg("event flow published an event")

	return nil
}
