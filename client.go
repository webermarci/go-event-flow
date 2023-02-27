package eventFlow

import (
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/rs/zerolog/log"
)

type ClientConfig struct {
	URL      string
	Username string
	Password string
}

type Client struct {
	name       string
	mqttClient mqtt.Client
}

func NewClient(name string, config ClientConfig) *Client {
	mqttConfig := mqtt.NewClientOptions()
	mqttConfig.SetKeepAlive(3 * time.Second)
	mqttConfig.SetAutoReconnect(true)
	mqttConfig.SetConnectTimeout(5 * time.Second)
	mqttConfig.SetPingTimeout(5 * time.Second)

	mqttConfig.AddBroker(config.URL)
	mqttConfig.SetUsername(config.Username)
	mqttConfig.SetPassword(config.Password)

	mqttConfig.SetConnectionLostHandler(func(c mqtt.Client, err error) {
		log.Warn().
			Err(err).
			Str("client", name).
			Msg("MQTT client connection lost")
	})

	mqttConfig.SetReconnectingHandler(func(c mqtt.Client, co *mqtt.ClientOptions) {
		log.Info().
			Str("client", name).
			Msg("MQTT client is trying to reconnect")
	})

	mqttConfig.SetOnConnectHandler(func(c mqtt.Client) {
		log.Info().
			Str("client", name).
			Msg("MQTT client is connected")
	})

	mqttClient := mqtt.NewClient(mqttConfig)

	return &Client{
		name:       name,
		mqttClient: mqttClient,
	}
}

func (client *Client) Connect() error {
	token := client.mqttClient.Connect()
	token.Wait()

	if token.Error() != nil {
		log.Warn().Err(token.Error()).
			Str("client", client.name).
			Msg("MQTT client failed to connect")
		return token.Error()
	}

	return nil
}

func (client *Client) Disconnect() {
	client.mqttClient.Disconnect(1000)
	log.Info().
		Str("client", client.name).
		Msg("MQTT client is disconnected")
}
