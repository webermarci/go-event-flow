package eventFlow

import (
	"testing"
	"time"
)

func TestSubscription(t *testing.T) {
	client := NewClient(t.Name(), ClientConfig{
		URL: "tcp://test.mosquitto.org:1883",
	})

	if err := client.Connect(); err != nil {
		t.Fatal(err)
	}

	defer client.Disconnect()

	var eventType EventType = "emq/test/" + EventType(t.Name())

	flow := NewEventFlow[int](client, eventType, AtLeastOnce)

	if err := flow.Subscribe(func(event Event[int]) {}); err != nil {
		t.Fatal(err)
	}

	if err := flow.Unsubscribe(); err != nil {
		t.Fatal(err)
	}
}

func TestSubscriptionWithoutConnection(t *testing.T) {
	client := NewClient(t.Name(), ClientConfig{
		URL: "",
	})

	if err := client.Connect(); err == nil {
		t.Fatal("expected error")
	}

	var eventType EventType = "emq/test/" + EventType(t.Name())

	flow := NewEventFlow[int](client, eventType, AtLeastOnce)

	if err := flow.Subscribe(func(event Event[int]) {}); err == nil {
		t.Fatal("expected error")
	}
}

func TestUnsubscribtionWithoutConnection(t *testing.T) {
	client := NewClient(t.Name(), ClientConfig{
		URL: "",
	})

	if err := client.Connect(); err == nil {
		t.Fatal("expected error")
	}

	var eventType EventType = "emq/test/" + EventType(t.Name())

	flow := NewEventFlow[int](client, eventType, AtLeastOnce)

	if err := flow.Unsubscribe(); err == nil {
		t.Fatal("expected error")
	}
}

func TestPublishing(t *testing.T) {
	client := NewClient(t.Name(), ClientConfig{
		URL: "tcp://test.mosquitto.org:1883",
	})

	if err := client.Connect(); err != nil {
		t.Fatal(err)
	}

	defer client.Disconnect()

	var eventType EventType = "emq/test/" + EventType(t.Name())

	flow := NewEventFlow[int](client, eventType, AtLeastOnce)

	if err := flow.Publish(42); err != nil {
		t.Fatal(err)
	}
}

func TestPublishingWithoutConnection(t *testing.T) {
	client := NewClient(t.Name(), ClientConfig{
		URL: "",
	})

	if err := client.Connect(); err == nil {
		t.Fatal("error expected")
	}

	var eventType EventType = "emq/test/" + EventType(t.Name())

	flow := NewEventFlow[int](client, eventType, AtLeastOnce)

	if err := flow.Publish(42); err == nil {
		t.Fatal("error expected")
	}
}

func TestListening(t *testing.T) {
	client := NewClient(t.Name(), ClientConfig{
		URL: "tcp://test.mosquitto.org:1883",
	})

	if err := client.Connect(); err != nil {
		t.Fatal(err)
	}

	defer client.Disconnect()

	var eventType EventType = "emq/test/" + EventType(t.Name())

	flow := NewEventFlow[int](client, eventType, AtLeastOnce)

	received := false

	if err := flow.Subscribe(func(event Event[int]) {
		received = true
	}); err != nil {
		t.Fatal(err)
	}

	defer func() {
		if err := flow.Unsubscribe(); err != nil {
			t.Fatal(err)
		}
	}()

	go func() {
		if err := flow.Publish(42); err != nil {
			t.Fail()
		}
	}()

	time.Sleep(500 * time.Millisecond)

	if !received {
		t.Fatal("not received")
	}
}

func TestPublishingInvalidStruct(t *testing.T) {
	client := NewClient(t.Name(), ClientConfig{
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

	flow := NewEventFlow[Invalid](client, eventType, AtLeastOnce)

	if err := flow.Publish(Invalid{Channel: make(chan int)}); err == nil {
		t.Fatal("expected error")
	}
}
