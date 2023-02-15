package eventFlow

import (
	"testing"
	"time"
)

func TestSubscription(t *testing.T) {
	client := NewClient(ClientConfig{
		URL: "tcp://test.mosquitto.org:1883",
	})

	if err := client.Connect(); err != nil {
		t.Fatal(err)
	}

	defer client.Disconnect()

	var eventType EventType = "emq/test/" + EventType(t.Name())

	listener := NewEventListener[int](client, eventType, AtLeastOnce)

	if _, err := listener.Subscribe(); err != nil {
		t.Fatal(err)
	}

	if err := listener.Unsubscribe(); err != nil {
		t.Fatal(err)
	}
}

func TestSubscriptionWithoutConnection(t *testing.T) {
	client := NewClient(ClientConfig{
		URL: "",
	})

	if err := client.Connect(); err == nil {
		t.Fatal("expected error")
	}

	var eventType EventType = "emq/test/" + EventType(t.Name())

	listener := NewEventListener[int](client, eventType, AtLeastOnce)

	if _, err := listener.Subscribe(); err == nil {
		t.Fatal("expected error")
	}
}

func TestUnsubscribtionWithoutConnection(t *testing.T) {
	client := NewClient(ClientConfig{
		URL: "",
	})

	if err := client.Connect(); err == nil {
		t.Fatal("expected error")
	}

	var eventType EventType = "emq/test/" + EventType(t.Name())

	listener := NewEventListener[int](client, eventType, AtLeastOnce)

	if err := listener.Unsubscribe(); err == nil {
		t.Fatal("expected error")
	}
}

func TestPublishing(t *testing.T) {
	client := NewClient(ClientConfig{
		URL: "tcp://test.mosquitto.org:1883",
	})

	if err := client.Connect(); err != nil {
		t.Fatal(err)
	}

	defer client.Disconnect()

	var eventType EventType = "emq/test/" + EventType(t.Name())

	listener := NewEventListener[int](client, eventType, AtLeastOnce)

	if err := listener.Publish(42); err != nil {
		t.Fatal(err)
	}
}

func TestPublishingWithoutConnection(t *testing.T) {
	client := NewClient(ClientConfig{
		URL: "",
	})

	if err := client.Connect(); err == nil {
		t.Fatal("error expected")
	}

	var eventType EventType = "emq/test/" + EventType(t.Name())

	listener := NewEventListener[int](client, eventType, AtLeastOnce)

	if err := listener.Publish(42); err == nil {
		t.Fatal("error expected")
	}
}

func TestListening(t *testing.T) {
	client := NewClient(ClientConfig{
		URL: "tcp://test.mosquitto.org:1883",
	})

	if err := client.Connect(); err != nil {
		t.Fatal(err)
	}

	defer client.Disconnect()

	var eventType EventType = "emq/test/" + EventType(t.Name())

	listener := NewEventListener[int](client, eventType, AtLeastOnce)

	channel, err := listener.Subscribe()
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		if err := listener.Unsubscribe(); err != nil {
			t.Fatal(err)
		}
	}()

	go func() {
		if err := listener.Publish(42); err != nil {
			t.Fail()
		}
	}()

	select {
	case event := <-channel:
		if event.Payload != 42 {
			t.Fatal("invalid payload")
		}
		return
	case <-time.After(3 * time.Second):
		t.Fatal("timeout")
	}
}

func TestPublishingInvalidStruct(t *testing.T) {
	client := NewClient(ClientConfig{
		URL: "tcp://test.mosquitto.org:1883",
	})

	if err := client.Connect(); err != nil {
		t.Fatal(err)
	}

	defer client.Disconnect()

	var eventType EventType = "emq/test/" + EventType(t.Name())

	type Invalid struct {
		Channel chan int `json:"channel"`
	}

	listener := NewEventListener[Invalid](client, eventType, AtLeastOnce)

	if err := listener.Publish(Invalid{Channel: make(chan int)}); err == nil {
		t.Fatal("expected error")
	}
}
