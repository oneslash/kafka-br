package cmd

import (
	"os"

	"github.com/spf13/cobra"
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "kafka-br",
	Short: "Simple Kafka Backup and Restore",
	Long: `Simple Kafka Backup and Restore. It saves messages into file and also restores messages from file.`,
}

func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}
