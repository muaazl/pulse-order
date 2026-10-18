package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds all environment-driven settings for the PulseOrder services.
// Centralizing configuration ensures 12-Factor App compliance and prevents
// scattered os.Getenv calls across hot-path request handlers.
type Config struct {
	HTTPPort             string
	AdminAPIKey          string
	RedisAddr            string
	RedisPassword        string
	RedisDB              int
	KafkaBrokers         []string
	ElasticsearchURL     string
	RateLimitRequests    int
	RateLimitWindow      time.Duration
	IdempotencyTTL       time.Duration
	EnableGoSurgeEngine  bool
	PaymentDeclineRate   float64
	ShutdownTimeout      time.Duration
}

// Load reads environment variables and falls back to sensible local development defaults.
func Load() *Config {
	brokersRaw := getEnv("KAFKA_BROKERS", "localhost:9092")
	brokers := strings.Split(brokersRaw, ",")
	for i := range brokers {
		brokers[i] = strings.TrimSpace(brokers[i])
	}

	return &Config{
		HTTPPort:            getEnv("HTTP_PORT", "8080"),
		AdminAPIKey:         getEnv("ADMIN_API_KEY", "pulse-secret-admin-key"),
		RedisAddr:           getEnv("REDIS_ADDR", "localhost:6379"),
		RedisPassword:       getEnv("REDIS_PASSWORD", ""),
		RedisDB:             getEnvInt("REDIS_DB", 0),
		KafkaBrokers:        brokers,
		ElasticsearchURL:    getEnv("ELASTICSEARCH_URL", "http://localhost:9200"),
		RateLimitRequests:   getEnvInt("RATE_LIMIT_REQUESTS", 10),
		RateLimitWindow:     time.Duration(getEnvInt("RATE_LIMIT_WINDOW_SEC", 10)) * time.Second,
		IdempotencyTTL:      time.Duration(getEnvInt("IDEMPOTENCY_TTL_SEC", 300)) * time.Second,
		EnableGoSurgeEngine: getEnvBool("ENABLE_GO_SURGE_ENGINE", true),
		PaymentDeclineRate:  getEnvFloat("PAYMENT_DECLINE_RATE", 0.0),
		ShutdownTimeout:     time.Duration(getEnvInt("SHUTDOWN_TIMEOUT_SEC", 15)) * time.Second,
	}
}

func getEnv(key, fallback string) string {
	if val, ok := os.LookupEnv(key); ok && strings.TrimSpace(val) != "" {
		return val
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	valStr, ok := os.LookupEnv(key)
	if !ok {
		return fallback
	}
	parsed, err := strconv.Atoi(strings.TrimSpace(valStr))
	if err != nil {
		return fallback
	}
	return parsed
}

func getEnvFloat(key string, fallback float64) float64 {
	valStr, ok := os.LookupEnv(key)
	if !ok {
		return fallback
	}
	parsed, err := strconv.ParseFloat(strings.TrimSpace(valStr), 64)
	if err != nil {
		return fallback
	}
	return parsed
}

func getEnvBool(key string, fallback bool) bool {
	valStr, ok := os.LookupEnv(key)
	if !ok {
		return fallback
	}
	parsed, err := strconv.ParseBool(strings.TrimSpace(valStr))
	if err != nil {
		return fallback
	}
	return parsed
}

