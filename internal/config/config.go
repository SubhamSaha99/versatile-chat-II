package config

import (
	"fmt"
	"time"

	"github.com/ilyakaznacheev/cleanenv"
)

type EnvConfig struct {
	Port               string        `env:"PORT" env-default:"8083"`
	MongoURI           string        `env:"MONGO_URI" env-default:"mongodb://root:root@localhost:27017/?authSource=admin"`
	MongoDatabase      string        `env:"MONGO_DATABASE" env-default:"versatile_chat"`
	JWTSecret          string        `env:"JWT_SECRET" env-default:"abcdefABCDEF123456"`
	JWTExpiration      time.Duration `env:"JWT_EXPIRATION" env-default:"2h"`
	KafkaBrokerUrl     string        `env:"KAFKA_BROKER_URL" env-default:"localhost:29092"`
	KafkaTopic         string        `env:"KAFKA_TOPIC" env-default:"message_service_group"`
	KafkaFlushInterval string        `env:"KAFKA_FLUSH_INTERVAL_MS" env-default:"5000"`
	KafkaBatchSize     string        `env:"KAFKA_BATCH_SIZE" env-default:"10"`
	KafkaConsumerGroup string        `env:"KAFKA_CONSUMER_GROUP" env-default:"message-mongo-writer"`
}

// Function to load ENVs
func LoadEnv() (*EnvConfig, error) {
	var config EnvConfig

	if err := cleanenv.ReadConfig("./.env", &config); err != nil {
		return nil, fmt.Errorf("Error loading env %w", err)
	}
	return &config, nil
}
