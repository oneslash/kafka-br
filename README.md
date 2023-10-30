# Kafka Backup and Restore

It is a simple cli tool to backup all events in the Kafka cluster and then republish (restore) in another one.

## Usage

```bash
kafkabr <command> <flags>
```

## Commands


| Command | Description |
| -------- | -------- |
| backup | Backup all events available into file |
| restore | Restore from file all events |

# Backup Command

The backup command is used to backup messages to a file from a message broker.

## Usage

| Flag | Shorthand | Description | 
| -------- | -------- | -------- |
| `--topic` | `-t`| Topic to backup |
| `--output` | `-o` | Output file |
| `--broker`   | `-b` | Broker to connect to |
| `--username`   | `-u` | Username to connect to broker |
| `--password`   | `-p` | Password to connect to broker |



# Restore Command

The restore command is used to restore messages from a file to a message broker.

## Usage

| Flag | Shorthand | Description | 
| -------- | -------- | -------- |
| `--topic` | `-t`| Topic to restore (the topic should be available or auto create should be enabled) |
| `--input` | `-i` | Input file |
| `--broker`   | `-b` | Broker to connect to |
| `--username`   | `-u` | Username to connect to broker |
| `--password`   | `-p` | Password to connect to broker |

