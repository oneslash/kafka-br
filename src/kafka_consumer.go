package src

import (
	"fmt"
	"log"
	"os"

	"github.com/IBM/sarama"
)

// KafkaConsumer is a wrapper around sarama.Consumer
type KafkaConsumer struct {
	consumer sarama.Consumer
}

func NewKafkaConsumer(bootstrap []string) *KafkaConsumer {
	conf := sarama.NewConfig()
	consumer, err := sarama.NewConsumer(bootstrap, conf)
	if err != nil {
		panic(err)
	}

	return &KafkaConsumer{
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

	partitions, err := k.consumer.Partitions(topic)
	if err != nil {
		log.Printf("Failed to get the list of partitions:", err)
		return err
	}

	for _, partition := range partitions {
		pc, err := k.consumer.ConsumePartition(topic, partition, sarama.OffsetOldest)
		if err != nil {
			log.Printf("Failed to start consumer for partition", partition, ":", err)
		}
		defer pc.Close()

		for message := range pc.Messages() {
			_, err := out.WriteString(fmt.Sprintf("%s\n", message.Value))
			if err != nil {
				log.Printf("Failed to write to backup file:", err)
			}
		}
	}

	return nil
}

func (k *KafkaConsumer) Close() {
	k.consumer.Close()
}