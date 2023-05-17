package eventFlow

import (
	"testing"
)

func TestClientConnect(t *testing.T) {
	client := NewClient().Configure(ClientConfig{
		URL: "tcp://test.mosquitto.org:1883",
	})

	if err := client.Connect(); err != nil {
		t.Fatal(err)
	}

	client.Disconnect()
}

func TestClientFailedToConnect(t *testing.T) {
	client := NewClient().Configure(ClientConfig{
		URL: "",
	})

	if err := client.Connect(); err == nil {
		t.Fatal("error expected")
	}
}
