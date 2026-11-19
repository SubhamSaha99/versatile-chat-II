package config

import (
	"fmt"
	"time"

	"github.com/ilyakaznacheev/cleanenv"
)

type EnvConfig struct {
	Port          string `env:"PORT" env-default:"8083"`
	MongoURI      string `env:"MONGO_URI" env-default:"mongodb://root:root@localhost:27017/?authSource=admin"`
	MongoDatabase string `env:"MONGO_DATABASE" env-default:"versatile_chat"`
	JWTSecret string `env:"JWT_SECRET" env-default:"abcdefABCDEF123456"`
	JWTExpiration time.Duration `env:"JWT_EXPIRATION" env-deafault:"2h"`
}

// Function to load ENVs
func LoadEnv() (*EnvConfig, error) {
	var config EnvConfig

	if err := cleanenv.ReadConfig("./.env", &config); err != nil {
		return nil, fmt.Errorf("Error loading env %w", err)
	}
	return &config, nil
}
