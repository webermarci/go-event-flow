package eventFlow

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestIsSuccessful(t *testing.T) {
	var eventType EventType = "emq/test/" + EventType(t.Name())
	e1 := NewEvent(t.Name(), eventType, 1, nil)

	fmt.Println(e1.ID)

	if e1.IsSuccessful() == false {
		t.Fatal("expected true")
	}

	e2 := NewEvent(t.Name(), eventType, 1, errors.New(t.Name()))

	if e2.IsSuccessful() == true {
		t.Fatal("expected false")
	}
}

func TestSubscription(t *testing.T) {
	client := NewClient().Configure(ClientConfig{
		URL: "tcp://test.mosquitto.org:1883",
	})

	if err := client.Connect(); err != nil {
		t.Fatal(err)
	}

	defer client.Disconnect()

	var eventType EventType = "emq/test/" + EventType(t.Name())

	flow := NewEventFlow[int](client, t.Name(), eventType, AtLeastOnce)

	flow.SetCallback(func(event Event[int]) {})

	if err := flow.Subscribe(); err != nil {
		t.Fatal(err)
	}

	if err := flow.Unsubscribe(); err != nil {
		t.Fatal(err)
	}
}

func TestSubscriptionWithoutConnection(t *testing.T) {
	client := NewClient().Configure(ClientConfig{
		URL: "",
	})

	if err := client.Connect(); err == nil {
		t.Fatal("expected error")
	}

	var eventType EventType = "emq/test/" + EventType(t.Name())

	flow := NewEventFlow[int](client, t.Name(), eventType, AtLeastOnce)

	flow.SetCallback(func(event Event[int]) {})

	if err := flow.Subscribe(); err == nil {
		t.Fatal("expected error")
	}
}

func TestUnsubscribtionWithoutConnection(t *testing.T) {
	client := NewClient().Configure(ClientConfig{
		URL: "",
	})

	if err := client.Connect(); err == nil {
		t.Fatal("expected error")
	}

	var eventType EventType = "emq/test/" + EventType(t.Name())

	flow := NewEventFlow[int](client, t.Name(), eventType, AtLeastOnce)

	if err := flow.Unsubscribe(); err == nil {
		t.Fatal("expected error")
	}
}

func TestPublishingSuccesful(t *testing.T) {
	client := NewClient().Configure(ClientConfig{
		URL: "tcp://test.mosquitto.org:1883",
	})

	if err := client.Connect(); err != nil {
		t.Fatal(err)
	}

	defer client.Disconnect()

	var eventType EventType = "emq/test/" + EventType(t.Name())

	flow := NewEventFlow[int](client, t.Name(), eventType, AtLeastOnce)

	if err := flow.Publish(42, nil); err != nil {
		t.Fatal(err)
	}
}

func TestPublishingFailed(t *testing.T) {
	client := NewClient().Configure(ClientConfig{
		URL: "tcp://test.mosquitto.org:1883",
	})

	if err := client.Connect(); err != nil {
		t.Fatal(err)
	}

	defer client.Disconnect()

	var eventType EventType = "emq/test/" + EventType(t.Name())

	flow := NewEventFlow[int](client, t.Name(), eventType, AtLeastOnce)

	if err := flow.Publish(42, errors.New(t.Name())); err != nil {
		t.Fatal(err)
	}
}

func TestPublishingWithoutConnection(t *testing.T) {
	client := NewClient().Configure(ClientConfig{
		URL: "",
	})

	if err := client.Connect(); err == nil {
		t.Fatal("error expected")
	}

	var eventType EventType = "emq/test/" + EventType(t.Name())

	flow := NewEventFlow[int](client, t.Name(), eventType, AtLeastOnce)

	if err := flow.Publish(42, nil); err == nil {
		t.Fatal("error expected")
	}
}

func TestListening(t *testing.T) {
	client := NewClient().Configure(ClientConfig{
		URL: "tcp://test.mosquitto.org:1883",
	})

	if err := client.Connect(); err != nil {
		t.Fatal(err)
	}

	defer client.Disconnect()

	var eventType EventType = "emq/test/" + EventType(t.Name())

	wg := sync.WaitGroup{}
	wg.Add(1)

	flow := NewEventFlow[int](client, t.Name(), eventType, AtLeastOnce)

	flow.SetCallback(func(event Event[int]) {
		wg.Done()
	})

	if err := flow.Subscribe(); err != nil {
		t.Fatal(err)
	}

	defer func() {
		if err := flow.Unsubscribe(); err != nil {
			t.Fatal(err)
		}
	}()

	go func() {
		if err := flow.Publish(42, nil); err != nil {
			t.Fail()
		}
	}()

	wgChannel := make(chan struct{})

	go func() {
		defer close(wgChannel)
		wg.Wait()
	}()

	select {
	case <-wgChannel:
	case <-time.After(3 * time.Second):
		t.Fatal("timeout")
	}
}

func TestPublishingInvalidStruct(t *testing.T) {
	client := NewClient().Configure(ClientConfig{
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

	flow := NewEventFlow[Invalid](client, t.Name(), eventType, AtLeastOnce)

	if err := flow.Publish(Invalid{Channel: make(chan int)}, nil); err == nil {
		t.Fatal("expected error")
	}
}
