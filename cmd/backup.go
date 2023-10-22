package cmd

import (
	"github.com/spf13/cobra"
)

var backupCmd = &cobra.Command{
	Use:   "backup",
	Short: "Backup a kafka topic",
	Run: backupFn,
}

func init() {
	rootCmd.AddCommand(backupCmd)
	
	backupCmd.Flags().StringP("topic", "t", "", "Topic to backup")
	backupCmd.Flags().StringP("output", "o", "", "Output file")
	backupCmd.Flags().StringP("broker", "b", "", "Broker to connect to")
}

func backupFn(cmd *cobra.Command, args []string) {
}
