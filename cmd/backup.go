package cmd

import (
	"github.com/spf13/cobra"

	"kafka-br/src"
)

var backupCmd = &cobra.Command{
	Use:   "backup",
	Short: "Backup a kafka topic",
	Run:   backupFn,
}

func init() {
	rootCmd.AddCommand(backupCmd)

	backupCmd.Flags().StringP("topic", "t", "", "Topic to backup")
	backupCmd.Flags().StringP("output", "o", "", "Output file")
	backupCmd.Flags().StringP("broker", "b", "", "Broker to connect to")
	backupCmd.Flags().StringP("username", "u", "", "Username to connect to broker")
	backupCmd.Flags().StringP("password", "p", "", "Password to connect to broker")
}

func backupFn(cmd *cobra.Command, args []string) {
	topic, _ := cmd.Flags().GetString("topic")
	output, _ := cmd.Flags().GetString("output")
	broker, _ := cmd.Flags().GetString("broker")
	username, _ := cmd.Flags().GetString("username")
	password, _ := cmd.Flags().GetString("password")

	var sasl *src.SASLConfig
	if username != "" && password != "" {
		sasl = &src.SASLConfig{
			Username: username,
			Password: password,
		}
	}

	bootstrap := []string{broker}
	consumer := src.NewKafkaConsumer(bootstrap, sasl)
	defer consumer.Close()
	consumer.Backup(topic, output)
}
