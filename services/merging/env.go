package merging

type Env struct {
	// BYBIT_API_KEY        string
	// BYBIT_API_SECRET     string
	SUBGRAPH_API_TOKEN string

	POSTGRES_HOST     string
	POSTGRES_PORT     string
	POSTGRES_USER     string
	POSTGRES_PASSWORD string
	POSTGRES_DB_NAME  string
	POSTGRES_SSL_MODE string

	// ETH_MAINNET_RPC_WS   string
	// ETH_MAINNET_RPC_HTTP string
	// BSC_MAINNET_RPC      string

	// KAFKA_SERVER string
	// KAFKA_UPDATE_V3_POOLS_TOPIC string

	REDIS_SERVER string
}
