module github.com/airwallex/heartbeat/services/ingestion

go 1.22

require github.com/airwallex/heartbeat/pkg v0.0.0

require (
	github.com/klauspost/compress v1.17.7 // indirect
	github.com/pierrec/lz4/v4 v4.1.21 // indirect
	github.com/segmentio/kafka-go v0.4.47 // indirect
	github.com/stretchr/testify v1.9.0 // indirect
)

replace github.com/airwallex/heartbeat/pkg => ../../pkg
