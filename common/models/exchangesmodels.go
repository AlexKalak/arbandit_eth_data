package models

import (
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"slices"
)

var UNISWAP_ALL_COLUMNS []string = []string{
	UNISWAP_V3_POOL_TABLE,
	UNISWAP_V3_POOL_ADDRESS,
	UNISWAP_V3_POOL_EXCHANGE_NAME,
	UNISWAP_V3_POOL_CHAINID,
	UNISWAP_V3_POOL_TOKEN0_ADDRESS,
	UNISWAP_V3_POOL_TOKEN1_ADDRESS,
	UNISWAP_V3_POOL_SQRTPRICEX96,
	UNISWAP_V3_POOL_LIQUIDITY,
	UNISWAP_V3_POOL_TICK,
	UNISWAP_V3_POOL_TICK_SPACING,
	UNISWAP_V3_POOL_TICK_LOWER,
	UNISWAP_V3_POOL_TICK_UPPER,
	UNISWAP_V3_POOL_TICKS,
	UNISWAP_V3_POOL_FEE_TIER,
	UNISWAP_V3_POOL_IS_DUSTY,
	UNISWAP_V3_POOL_BLOCK_NUMBER,
	UNISWAP_V3_POOL_TOKEN0_HOLDING,
	UNISWAP_V3_POOL_TOKEN1_HOLDING,
	UNISWAP_V3_ZFO_10USD_RATE,
	UNISWAP_V3_NON_ZFO_10USD_RATE,
}

const UNISWAP_V3_POOL_TABLE = "uniswap_v3_pools"

const UNISWAP_V3_POOL_ADDRESS = "address"
const UNISWAP_V3_POOL_EXCHANGE_NAME = "exchange_name"
const UNISWAP_V3_POOL_CHAINID = "chain_id"
const UNISWAP_V3_POOL_TOKEN0_ADDRESS = "token0_address"
const UNISWAP_V3_POOL_TOKEN1_ADDRESS = "token1_address"
const UNISWAP_V3_POOL_SQRTPRICEX96 = "sqrt_price_x96"
const UNISWAP_V3_POOL_LIQUIDITY = "liquidity"

const UNISWAP_V3_POOL_TICK = "tick"
const UNISWAP_V3_POOL_TICK_SPACING = "tick_spacing"
const UNISWAP_V3_POOL_TICK_LOWER = "tick_lower"
const UNISWAP_V3_POOL_TICK_UPPER = "tick_upper"
const UNISWAP_V3_POOL_TICKS = "ticks"

const UNISWAP_V3_POOL_FEE_TIER = "fee_tier"
const UNISWAP_V3_POOL_IS_DUSTY = "is_dusty"
const UNISWAP_V3_POOL_BLOCK_NUMBER = "block_number"

const UNISWAP_V3_POOL_TOKEN0_HOLDING = "token0_holding"
const UNISWAP_V3_POOL_TOKEN1_HOLDING = "token1_holding"

const UNISWAP_V3_ZFO_10USD_RATE = "zfo_10usd_rate"
const UNISWAP_V3_NON_ZFO_10USD_RATE = "non_zfo_10usd_rate"

type UniswapV3PoolTick struct {
	TickIdx      int      `json:"tick_idx"`
	LiquidityNet *big.Int `json:"liquidity_net"`
}

type UniswapV3Pool struct {
	Address      string
	ExchangeName string
	ChainID      uint
	Token0       string
	Token1       string
	SqrtPriceX96 *big.Int
	Liquidity    *big.Int
	Tick         int
	TickSpacing  int
	TickLower    int
	TickUpper    int
	FeeTier      int
	IsDusty      bool
	BlockNumber  int
	ticks        []UniswapV3PoolTick

	Zfo10USDRate    *big.Float
	NonZfo10USDRate *big.Float
}

type UniswapV3PoolDTO struct {
	Address      string
	ExchangeName string
	ChainID      uint
	Token0       string
	Token1       string
	SqrtPriceX96 *big.Int
	Liquidity    *big.Int
	Tick         int
	TickSpacing  int
	TickLower    int
	TickUpper    int
	FeeTier      int
	IsDusty      bool
	BlockNumber  int
	Ticks        []UniswapV3PoolTick `json:"ticks"`

	Zfo10USDRate    *big.Float
	NonZfo10USDRate *big.Float
}

func (u *UniswapV3PoolDTO) FromModel(orig *UniswapV3Pool) {
	u.Address = orig.Address
	u.ExchangeName = orig.ExchangeName
	u.ChainID = orig.ChainID
	u.Token0 = orig.Token0
	u.Token1 = orig.Token1
	u.SqrtPriceX96 = orig.SqrtPriceX96
	u.Liquidity = orig.Liquidity
	u.Tick = orig.Tick
	u.TickSpacing = orig.TickSpacing
	u.TickLower = orig.TickLower
	u.TickUpper = orig.TickUpper
	u.FeeTier = orig.FeeTier
	u.IsDusty = orig.IsDusty
	u.BlockNumber = orig.BlockNumber
	u.Ticks = orig.ticks

	u.Zfo10USDRate = orig.Zfo10USDRate
	u.NonZfo10USDRate = orig.NonZfo10USDRate
}

func (u *UniswapV3Pool) FromDTO(dto *UniswapV3PoolDTO) {
	u.Address = dto.Address
	u.ExchangeName = dto.ExchangeName
	u.ChainID = dto.ChainID
	u.Token0 = dto.Token0
	u.Token1 = dto.Token1
	u.SqrtPriceX96 = dto.SqrtPriceX96
	u.Liquidity = dto.Liquidity
	u.Tick = dto.Tick
	u.TickSpacing = dto.TickSpacing
	u.TickLower = dto.TickLower
	u.TickUpper = dto.TickUpper
	u.FeeTier = dto.FeeTier
	u.IsDusty = dto.IsDusty
	u.BlockNumber = dto.BlockNumber
	u.ticks = dto.Ticks

	u.Zfo10USDRate = dto.Zfo10USDRate
	u.NonZfo10USDRate = dto.NonZfo10USDRate
}

func (u *UniswapV3Pool) NearTicksJSON() string {
	ticksJSON, err := json.Marshal(u.ticks)
	if err != nil {
		return ""
	}
	return string(ticksJSON)
}

func (u *UniswapV3Pool) GetTicks() []UniswapV3PoolTick {
	return u.ticks
}

func (u *UniswapV3Pool) SetTicks(ticks []UniswapV3PoolTick) {
	u.ticks = ticks
	u.SortTicks()
}

func (u *UniswapV3Pool) SortTicks() {
	isSorted := true
	for i := 0; i < len(u.ticks)-1; i++ {
		if u.ticks[i].TickIdx > u.ticks[i+1].TickIdx {
			isSorted = false
			break
		}
	}
	if isSorted {
		return
	}

	slices.SortFunc(u.ticks, func(t1, t2 UniswapV3PoolTick) int {
		return t1.TickIdx - t2.TickIdx
	})
}

func (p *UniswapV3Pool) GetIdentificator() V3PoolIdentificator {
	return V3PoolIdentificator{
		Address: p.Address,
		ChainID: p.ChainID,
	}
}

func (p *UniswapV3Pool) TicksValid() bool {
	if len(p.ticks) < 2 {
		return false
	}
	// if p.TickLower >= p.TickUpper {
	// 	return false
	// }

	return true
}

func (p *UniswapV3Pool) GetJSON() ([]byte, error) {
	dto := UniswapV3PoolDTO{}
	dto.FromModel(p)
	return json.Marshal(dto)
}

func (p *UniswapV3Pool) FillFromJSON(jsonBytes []byte) error {
	dto := UniswapV3PoolDTO{}
	err := json.Unmarshal(jsonBytes, &dto)
	p.FromDTO(&dto)

	return err
}

func (p *UniswapV3Pool) UpdateTickLowerUpper() {
	var lowerTick int64 = math.MinInt64
	var upperTick int64 = math.MaxInt64

	for _, tick := range p.ticks {
		if int64(tick.TickIdx) > lowerTick && tick.TickIdx < p.Tick {
			lowerTick = int64(tick.TickIdx)
		} else if tick.TickIdx > p.Tick && upperTick > int64(tick.TickIdx) {
			upperTick = int64(tick.TickIdx)
		}
	}

	if lowerTick > math.MinInt64 && upperTick < math.MaxInt64 {
		p.TickLower = int(lowerTick)
		p.TickUpper = int(upperTick)
	}
}

type V3PoolIdentificator struct {
	Address string
	ChainID uint
}

func (p V3PoolIdentificator) String() string {
	return fmt.Sprintf("%d.%s", p.ChainID, p.Address)
}

const UNISWAP_V2_PAIR_TABLE = "uniswap_v2_pairs"

const UNISWAP_V2_PAIR_ADDRESS = "address"
const UNISWAP_V2_PAIR_EXCHANGE_NAME = "exchange_name"
const UNISWAP_V2_PAIR_CHAINID = "chain_id"
const UNISWAP_V2_PAIR_TOKEN0_ADDRESS = "token0_address"
const UNISWAP_V2_PAIR_TOKEN1_ADDRESS = "token1_address"
const UNISWAP_V2_PAIR_AMOUNT0 = "amount0"
const UNISWAP_V2_PAIR_AMOUNT1 = "amount1"
const UNISWAP_V2_PAIR_BLOCK_NUMBER = "block_number"

const UNISWAP_V2_PAIR_FEE_TIER = "fee_tier"
const UNISWAP_V2_PAIR_IS_DUSTY = "is_dusty"

type UniswapV2Pair struct {
	Address      string
	ExchangeName string
	ChainID      uint
	Token0       string
	Token1       string
	Amount0      *big.Int
	Amount1      *big.Int
	FeeTier      int //format: 10000 = 1%
	IsDusty      bool
	BlockNumber  int
}

type V2PairIdentificator struct {
	Address string
	ChainID uint
}

func (p *UniswapV2Pair) GetIdentificator() V2PairIdentificator {
	return V2PairIdentificator{
		Address: p.Address,
		ChainID: p.ChainID,
	}
}

func (p V2PairIdentificator) String() string {
	return fmt.Sprintf("%d.%s", p.ChainID, p.Address)
}
