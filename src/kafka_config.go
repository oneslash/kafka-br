package src

import "github.com/IBM/sarama"

func makeConfig(bootstrap []string) *sarama.Config {
	var config =  &sarama.Config{}
	config.Metadata.Full = true
	return config
}
