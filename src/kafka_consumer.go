package src

import (
	"crypto/tls"
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/IBM/sarama"
)

// KafkaConsumer is a wrapper around sarama.Consumer
type KafkaConsumer struct {
	client   sarama.Client
	consumer sarama.Consumer
}

func NewKafkaConsumer(bootstrap []string, sasl *SASLConfig) *KafkaConsumer {
	sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
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

	client, err := sarama.NewClient(bootstrap, conf)
	if err != nil {
		panic(err)
	}

	consumer, err := sarama.NewConsumer(bootstrap, conf)
	if err != nil {
		panic(err)
	}

	return &KafkaConsumer{
		client:   client,
		consumer: consumer,
	}
}

// Connect to kafka and backup the topic to file
func (k *KafkaConsumer) Backup(topic string, output string) error {
	out, err := os.Create(output)
	if err != nil {
		log.Printf("Failed to create backup file:", err)
		return err
	}
	defer out.Close()

	partitions, err := k.client.Partitions(topic)
	if err != nil {
		log.Printf("Failed to get the list of partitions:", err)
		return err
	}

	wg := sync.WaitGroup{}
	for _, partition := range partitions {
		log.Printf("Starting consumer for partition %d", partition)
		pc, err := k.consumer.ConsumePartition(topic, partition, sarama.OffsetOldest)
		if err != nil {
			log.Print("Failed to start consumer for partition", partition, ":", err)
		}

		if count, _ := k.countMessagesInPartition(topic, partition); count > 0 {
			wg.Add(1)
			go func() {
				k.processMessages(pc, out, &wg)
			}()
		}
	}

	wg.Wait()
	return nil
}

func (k *KafkaConsumer) countMessagesInPartition(topic string, partition int32) (int64, error) {
	newestOffset, err := k.client.GetOffset(topic, partition, sarama.OffsetNewest)
	if err != nil {
		return 0, err
	}

	oldestOffset, err := k.client.GetOffset(topic, partition, sarama.OffsetOldest)
	if err != nil {
		return 0, err
	}

	return newestOffset - oldestOffset, nil
}

func (k *KafkaConsumer) processMessages(pc sarama.PartitionConsumer, out *os.File, wg *sync.WaitGroup) error {
	defer wg.Done()
	defer pc.Close()
	for message := range pc.Messages() {
		_, err := out.WriteString(fmt.Sprintf("%s\n", message.Value))
		if err != nil {
			log.Printf("Failed to write to backup file: %s", err)
		}
	}
	return nil
}

func (k *KafkaConsumer) Close() {
	k.client.Close()
}
