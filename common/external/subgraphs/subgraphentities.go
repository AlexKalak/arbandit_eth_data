package subgraphs

type TokenResponse struct {
	ID       string `json:"id"`
	Decimals string `json:"decimals"`
	Name     string `json:"name"`
	Symbol   string `json:"symbol"`
}

type PoolTickResponse struct {
	TickIdx      string `json:"tickIdx"`
	LiquidityNet string `json:"liquidityNet"`
	PoolAddress  string `json:"poolAddress"`
}

type PoolResponseToken struct {
	ID string `json:"id"`
}

type PoolResponse struct {
	ID      string            `json:"id"`
	FeeTier string            `json:"feeTier"`
	Token0  PoolResponseToken `json:"token0"`
	Token1  PoolResponseToken `json:"token1"`

	ExchangeName string
}
