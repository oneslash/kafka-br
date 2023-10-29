package cmd

import (
	"github.com/spf13/cobra"

	"kafka-br/src"
)

var restoreCmd = &cobra.Command{
	Use:   "restore",
	Short: "Restore a kafka topic",
	Run:   restoreFn,
}

func init() {
	rootCmd.AddCommand(restoreCmd)

	restoreCmd.Flags().StringP("topic", "t", "", "Topic to restore")
	restoreCmd.Flags().StringP("input", "i", "", "Input file")
	restoreCmd.Flags().StringP("broker", "b", "", "Broker to connect to")
	restoreCmd.Flags().StringP("username", "u", "", "Username to connect to broker")
	restoreCmd.Flags().StringP("password", "p", "", "Password to connect to broker")
}

func restoreFn(cmd *cobra.Command, args []string) {
	topic, _ := cmd.Flags().GetString("topic")
	input, _ := cmd.Flags().GetString("input")
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
	producer := src.NewKafkaProducer(bootstrap, sasl)
	defer producer.Close()
	producer.Restore(topic, input)
}
