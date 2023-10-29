package src

import (
	"bufio"
	"crypto/tls"
	"log"
	"os"

	"github.com/IBM/sarama"
)

type KafkaProducer struct {
	producer sarama.SyncProducer
}

func NewKafkaProducer(bootstrap []string, sasl *SASLConfig) *KafkaProducer {
	conf := sarama.NewConfig()
	if sasl != nil {
		conf.Net.TLS.Enable = true
		conf.Net.TLS.Config = &tls.Config{
			InsecureSkipVerify: true, // This skips certificate verification, consider adding the Confluent Root CA for production scenarios.
		}
		conf.Net.SASL.Enable = true
		conf.Net.SASL.User = sasl.Username
		conf.Net.SASL.Password = sasl.Password
		conf.Net.SASL.Mechanism = sarama.SASLTypePlaintext
		conf.ClientID = "kafka-br"
	}
	conf.Producer.Return.Successes = true
	producer, err := sarama.NewSyncProducer(bootstrap, conf)
	if err != nil {
		panic(err)
	}

	return &KafkaProducer{
		producer: producer,
	}
}

func (k *KafkaProducer) Restore(topic string, input string) error {
	file, err := os.Open(input)
	if err != nil {
		log.Print("Failed to open backup file:", err)
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		msg := &sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.StringEncoder(scanner.Text()),
		}

		_, _, err := k.producer.SendMessage(msg)
		if err != nil {
			log.Printf("Failed to produce message: %s", err)
		}
	}

	if err := scanner.Err(); err != nil {
		log.Print("Failed to read backup file:", err)
		return err
	}
	
	return nil
}

func (k *KafkaProducer) Close() error {
	k.producer.Close()
	return nil
}

