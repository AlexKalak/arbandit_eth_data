package models

import "math/big"

const (
	V3_TRANSACTION_TABLE                    = "v3_pool_transactions"
	V3_TRANSACTION_ID                       = "id"
	V3_TRANSACTION_CHAIN_ID                 = "chain_id"
	V3_TRANSACTION_POOL_ADDRESS             = "pool_address"
	V3_TRANSACTION_TX_HASH                  = "tx_hash"
	V3_TRANSACTION_BLOCK_NUMBER             = "block_number"
	V3_TRANSACTION_AMOUNT0                  = "amount0"
	V3_TRANSACTION_AMOUNT1                  = "amount1"
	V3_TRANSACTION_ARCHIVE_TOKEN0_USD_PRICE = "archive_token0_usd_price"
	V3_TRANSACTION_ARCHIVE_TOKEN1_USD_PRICE = "archive_token1_usd_price"
)

type V3Transaction struct {
	ID                    int
	PoolAddress           string
	ChainID               uint
	TxHash                string
	BlockNumber           uint64
	Amount0               *big.Int
	Amount1               *big.Int
	ArchiveToken0USDPrice *big.Float
	ArchiveToken1USDPrice *big.Float
}
