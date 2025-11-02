package poolupdaterservice

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"

	"github.com/alexkalak/go_market_analyze/common/core/exchangables/v3poolexchangable"
	"github.com/alexkalak/go_market_analyze/common/models"
	"github.com/ethereum/go-ethereum/common"
	"github.com/segmentio/kafka-go"
)

type poolEventMetaData struct {
	Type        string `json:"type"`
	BlockNumber uint64 `json:"block_number"`
	Address     string `json:"address"`
	TxHash      string `json:"tx_hash"`
}

type swapEventData struct {
	Sender       common.Address `json:"sender"`
	Recipient    common.Address `json:"repcipient"`
	Amount0      *big.Int       `json:"amount0"`
	Amount1      *big.Int       `json:"amount1"`
	SqrtPriceX96 *big.Int       `json:"sqrt_price_x96"`
	Liquidity    *big.Int       `json:"liquidity"`
	Tick         *big.Int       `json:"tick"`
}

type mintEventData struct {
	Sender    common.Address `json:"sender"`
	Owner     common.Address `json:"owner"`
	TickLower int32          `json:"tick_lower"`
	TickUpper int32          `json:"tick_upper"`
	Amount    *big.Int       `json:"amount"`
	Amount0   *big.Int       `json:"amount0"`
	Amount1   *big.Int       `json:"amount1"`
}

type burnEventData struct {
	Owner     common.Address `json:"owner"`
	TickLower int32          `json:"tick_lower"`
	TickUpper int32          `json:"tick_upper"`
	Amount    *big.Int       `json:"amount"`
	Amount0   *big.Int       `json:"amount0"`
	Amount1   *big.Int       `json:"amount1"`
}

type poolEventData[T swapEventData | mintEventData | burnEventData] struct {
	Data T `json:"data"`
}

func (s *poolUpdaterService) Start(ctx context.Context) error {
	chanel := make(chan *kafka.Message, 1024)
	go s.startPostgresUpdater(ctx, chanel)

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{s.config.KafkaServer},
		Topic:   s.config.KafkaUpdateV3PoolsTopic,
		GroupID: "ssanina",
	})

	defer reader.Close()

	fmt.Println("🚀 Listening for messages...")
	for {
		m, err := reader.ReadMessage(ctx)
		if err != nil {
			fmt.Println("failed to read message:", err)
			continue
		}

		err = s.handlePoolEventMessageForCache(&m)
		if err != nil {
			fmt.Println("Error parsing message: ", err)
			continue
		}

		chanel <- &m
	}
}

func (s *poolUpdaterService) handlePoolEventMessageForCache(m *kafka.Message) error {
	fmt.Println("handling message")
	if m == nil {
		return errors.New("nil message")
	}

	metaData := poolEventMetaData{}
	if err := json.Unmarshal(m.Value, &metaData); err != nil {
		return err
	}

	if metaData.Type == "BlockOver" {
		fmt.Println("BLOCK OVER: ", metaData.BlockNumber)
		fmt.Println(s.currentCheckingBlock, metaData.BlockNumber)

		if s.currentCheckingBlock == metaData.BlockNumber {
			pools := make([]models.UniswapV3Pool, 0, len(s.currentBlockPoolChanges))

			for _, pool := range s.currentBlockPoolChanges {
				pool.BlockNumber = int(metaData.BlockNumber)
				pools = append(pools, pool)
			}

			for vpi := range s.currentBlockPoolChanges {
				delete(s.currentBlockPoolChanges, vpi)
			}

			err := s.v3PoolCacheRepo.SetPools(s.config.ChainID, pools)
			if err != nil {
				return err
			}
			err = s.v3PoolCacheRepo.SetBlockNumber(s.config.ChainID, s.currentCheckingBlock)
			if err != nil {
				return err
			}
		}

		return nil
	}

	s.currentCheckingBlock = metaData.BlockNumber

	switch metaData.Type {
	case "Swap":
		data := poolEventData[swapEventData]{}
		if err := json.Unmarshal(m.Value, &data); err != nil {
			return err
		}

		poolIdentificator := models.V3PoolIdentificator{
			Address: metaData.Address,
			ChainID: s.config.ChainID,
		}

		pool := models.UniswapV3Pool{}

		if existingPool, ok := s.currentBlockPoolChanges[poolIdentificator]; ok {
			pool = existingPool
		} else {
			var err error
			pool, err = s.v3PoolCacheRepo.GetPoolByIdentificator(poolIdentificator)
			if err != nil {
				return err
			}
		}

		token0, ok0 := s.tokensMap[models.TokenIdentificator{Address: pool.Token0, ChainID: s.config.ChainID}]
		token1, ok1 := s.tokensMap[models.TokenIdentificator{Address: pool.Token1, ChainID: s.config.ChainID}]
		if !ok0 || !ok1 {
			return errors.New("pool tokens not found")
		}

		oldZfo := new(big.Float).Set(pool.Zfo10USDRate)

		//Updating zfo10usdrate and nonzfo10usdrate
		err := v3poolexchangable.UpdateRateFor10USD(&pool, token0, token1)
		if err != nil {
			pool.NonZfo10USDRate = big.NewFloat(0)
			pool.Zfo10USDRate = big.NewFloat(0)
		}

		fmt.Println("Old zfo 10 usd rate:", oldZfo)
		fmt.Println("Upd zfo 10 usd rate:", pool.Zfo10USDRate.String())

		pool.Liquidity = data.Data.Liquidity
		pool.SqrtPriceX96 = data.Data.SqrtPriceX96
		pool.Tick = int(data.Data.Tick.Int64())

		s.currentBlockPoolChanges[poolIdentificator] = pool

	case "Mint":
		data := poolEventData[mintEventData]{}
		if err := json.Unmarshal(m.Value, &data); err != nil {
			return err
		}

		poolIdentificator := models.V3PoolIdentificator{
			Address: metaData.Address,
			ChainID: s.config.ChainID,
		}

		pool := models.UniswapV3Pool{}
		if existingPool, ok := s.currentBlockPoolChanges[poolIdentificator]; ok {
			pool = existingPool
		} else {
			var err error
			pool, err = s.v3PoolCacheRepo.GetPoolByIdentificator(poolIdentificator)
			if err != nil {
				return err
			}
		}

		if data.Data.TickLower < int32(pool.Tick) && int32(pool.Tick) < data.Data.TickUpper {
			pool.Liquidity.Add(pool.Liquidity, data.Data.Amount)

			token0, ok0 := s.tokensMap[models.TokenIdentificator{Address: pool.Token0, ChainID: s.config.ChainID}]
			token1, ok1 := s.tokensMap[models.TokenIdentificator{Address: pool.Token1, ChainID: s.config.ChainID}]
			if !ok0 || !ok1 {
				return errors.New("pool tokens not found")
			}
			//Updating zfo10usdrate and nonzfo10usdrate
			err := v3poolexchangable.UpdateRateFor10USD(&pool, token0, token1)
			if err != nil {
				pool.NonZfo10USDRate = big.NewFloat(0)
				pool.Zfo10USDRate = big.NewFloat(0)
			}
		}

		s.currentBlockPoolChanges[poolIdentificator] = pool
	case "Burn":
		data := poolEventData[burnEventData]{}
		if err := json.Unmarshal(m.Value, &data); err != nil {
			return err
		}

		poolIdentificator := models.V3PoolIdentificator{
			Address: metaData.Address,
			ChainID: s.config.ChainID,
		}

		pool := models.UniswapV3Pool{}
		if existingPool, ok := s.currentBlockPoolChanges[poolIdentificator]; ok {
			pool = existingPool
		} else {
			var err error
			pool, err = s.v3PoolCacheRepo.GetPoolByIdentificator(poolIdentificator)
			if err != nil {
				return err
			}
		}

		if data.Data.TickLower < int32(pool.Tick) && int32(pool.Tick) < data.Data.TickUpper {
			pool.Liquidity.Sub(pool.Liquidity, data.Data.Amount)

			token0, ok0 := s.tokensMap[models.TokenIdentificator{Address: pool.Token0, ChainID: s.config.ChainID}]
			token1, ok1 := s.tokensMap[models.TokenIdentificator{Address: pool.Token1, ChainID: s.config.ChainID}]
			if !ok0 || !ok1 {
				return errors.New("Pool tokens not found")
			}

			//Updating zfo10usdrate and nonzfo10usdrate
			err := v3poolexchangable.UpdateRateFor10USD(&pool, token0, token1)
			if err != nil {
				pool.NonZfo10USDRate = big.NewFloat(0)
				pool.Zfo10USDRate = big.NewFloat(0)
			}
		}

		s.currentBlockPoolChanges[poolIdentificator] = pool

	default:
		return errors.New("message type not found")

	}

	return nil
}

func (s *poolUpdaterService) startPostgresUpdater(ctx context.Context, chanel <-chan *kafka.Message) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case m := <-chanel:
			err := s.handlePoolEventMessageForPostgres(m)
			if err != nil {
				fmt.Println("error handling postgres message: ", err)
				continue
			}

		}
	}

}

func (s *poolUpdaterService) handlePoolEventMessageForPostgres(m *kafka.Message) error {
	fmt.Println("handling postgres message")
	if m == nil {
		return errors.New("nil message")
	}

	metaData := poolEventMetaData{}
	if err := json.Unmarshal(m.Value, &metaData); err != nil {
		return err
	}

	switch metaData.Type {
	case "Swap":
		return s.handleSwapEventDB(metaData, m)

	case "Mint":
		return s.handleMintEventDB(metaData, m)

	case "Burn":
		return s.handleBurnEventDB(metaData, m)

	default:
		return errors.New("message type not found")
	}
}

func (s *poolUpdaterService) handleSwapEventDB(metaData poolEventMetaData, m *kafka.Message) error {
	data := poolEventData[swapEventData]{}
	if err := json.Unmarshal(m.Value, &data); err != nil {
		return err
	}

	poolIdentificator := models.V3PoolIdentificator{
		Address: metaData.Address,
		ChainID: s.config.ChainID,
	}

	pool, err := s.v3PoolDBRepo.GetPoolByPoolIdentificator(poolIdentificator)
	if err != nil {
		return err
	}

	newTick := int(data.Data.Tick.Int64())

	if newTick < pool.TickLower || newTick > pool.TickUpper {
		fmt.Println("Checking new tick: ", newTick)
		fmt.Println("Old ticks: ", pool.Tick, pool.TickLower, pool.TickUpper)

		var lowerTick int64 = math.MinInt64
		var upperTick int64 = math.MaxInt64

		initializedTicks := pool.NearTicks()
		fmt.Println("initializedTicks: ", initializedTicks)

		for _, tick := range initializedTicks {
			if int64(tick) > lowerTick && tick < newTick {
				lowerTick = int64(tick)
			} else if upperTick > int64(tick) && tick > newTick {
				upperTick = int64(tick)
			}
		}

		if lowerTick > math.MinInt64 && upperTick < math.MaxInt64 {
			pool.TickLower = int(lowerTick)
			pool.TickUpper = int(upperTick)
		}
	}

	pool.Tick = newTick
	fmt.Println("New ticks: ", pool.Tick, pool.TickLower, pool.TickUpper)

	oldZfo := new(big.Float)
	oldZfo.Set(pool.Zfo10USDRate)

	token0, ok0 := s.tokensMap[models.TokenIdentificator{Address: pool.Token0, ChainID: s.config.ChainID}]
	token1, ok1 := s.tokensMap[models.TokenIdentificator{Address: pool.Token1, ChainID: s.config.ChainID}]

	if !ok0 || !ok1 {
		return errors.New("pool tokens not found")
	}
	err = v3poolexchangable.UpdateRateFor10USD(&pool, token0, token1)

	if err != nil {
		pool.Zfo10USDRate = big.NewFloat(0)
		pool.NonZfo10USDRate = big.NewFloat(0)
		pool.IsDusty = true
	} else {
		fmt.Println("=====")
		fmt.Println("Addr: ", pool.Address)
		fmt.Println("Old zfo 10 usd rate:", oldZfo)
		fmt.Println("Upd zfo 10 usd rate:", pool.Zfo10USDRate.String())
		fmt.Println("=====")
	}

	pool.Liquidity = data.Data.Liquidity
	pool.SqrtPriceX96 = data.Data.SqrtPriceX96

	err = s.v3PoolDBRepo.UpdatePoolColumns(pool, []string{
		models.UNISWAP_V3_POOL_LIQUIDITY,
		models.UNISWAP_V3_POOL_SQRTPRICEX96,
		models.UNISWAP_V3_POOL_TICK,
		models.UNISWAP_V3_NON_ZFO_10USD_RATE,
		models.UNISWAP_V3_ZFO_10USD_RATE,
		models.UNISWAP_V3_POOL_BLOCK_NUMBER,
		models.UNISWAP_V3_POOL_IS_DUSTY,
	})
	if err != nil {
		return err
	}

	v3Tx := models.V3Transaction{
		TxHash:                metaData.TxHash,
		BlockNumber:           metaData.BlockNumber,
		Amount0:               data.Data.Amount0,
		Amount1:               data.Data.Amount1,
		ArchiveToken0USDPrice: token0.DefiUSDPrice,
		ArchiveToken1USDPrice: token1.DefiUSDPrice,
	}

	err = s.v3TransactionDBRepo.CreateTransaction(&v3Tx)
	if err != nil {
		fmt.Println("error creating tx:", err)
		return err
	}

	return nil
}

func (s *poolUpdaterService) handleMintEventDB(metaData poolEventMetaData, m *kafka.Message) error {
	data := poolEventData[mintEventData]{}
	if err := json.Unmarshal(m.Value, &data); err != nil {
		return err
	}

	fmt.Println("DB Mint")

	poolIdentificator := models.V3PoolIdentificator{
		Address: metaData.Address,
		ChainID: s.config.ChainID,
	}

	pool, err := s.v3PoolDBRepo.GetPoolByPoolIdentificator(poolIdentificator)
	if err != nil {
		return err
	}
	pool.BlockNumber = int(metaData.BlockNumber)

	initializedTicks := pool.NearTicks()

	newTickLower := int(data.Data.TickLower)
	newTickUpper := int(data.Data.TickUpper)
	newTicks := []int{newTickLower, newTickUpper}

	for newTick := range newTicks {
		for i := range initializedTicks {
			if i == len(initializedTicks)-1 {
				if newTick > initializedTicks[i] {
					initializedTicks = append(initializedTicks, newTick)
				}

				break
			}

			if newTick > initializedTicks[i] && newTick < initializedTicks[i+1] {
				initializedTicks = append(initializedTicks, initializedTicks[:i+1]...)
				initializedTicks = append(initializedTicks, newTick)
				initializedTicks = append(initializedTicks, initializedTicks[i+1:]...)
				break
			}

		}

	}

	if newTickLower < pool.Tick && pool.Tick < newTickUpper {
		token0, ok0 := s.tokensMap[models.TokenIdentificator{Address: pool.Token0, ChainID: s.config.ChainID}]
		token1, ok1 := s.tokensMap[models.TokenIdentificator{Address: pool.Token1, ChainID: s.config.ChainID}]
		if !ok0 || !ok1 {
			return errors.New("pool tokens not found")
		}
		err := v3poolexchangable.UpdateRateFor10USD(&pool, token0, token1)
		if err != nil {
			pool.Zfo10USDRate = big.NewFloat(0)
			pool.NonZfo10USDRate = big.NewFloat(0)
			pool.IsDusty = true
		}

		pool.Liquidity.Add(pool.Liquidity, data.Data.Amount)
	}

	err = s.v3PoolDBRepo.UpdatePoolColumns(pool, []string{
		models.UNISWAP_V3_POOL_LIQUIDITY,
		models.UNISWAP_V3_NON_ZFO_10USD_RATE,
		models.UNISWAP_V3_ZFO_10USD_RATE,
		models.UNISWAP_V3_POOL_BLOCK_NUMBER,
		models.UNISWAP_V3_POOL_IS_DUSTY,
	})

	if err != nil {
		return err
	}

	return nil
}

func (s *poolUpdaterService) handleBurnEventDB(metaData poolEventMetaData, m *kafka.Message) error {
	fmt.Println("DB Burn")
	data := poolEventData[burnEventData]{}
	if err := json.Unmarshal(m.Value, &data); err != nil {
		return err
	}

	poolIdentificator := models.V3PoolIdentificator{
		Address: metaData.Address,
		ChainID: s.config.ChainID,
	}

	pool, err := s.v3PoolDBRepo.GetPoolByPoolIdentificator(poolIdentificator)
	if err != nil {
		return err
	}
	pool.BlockNumber = int(metaData.BlockNumber)

	initializedTicks := pool.NearTicks()

	newTickLower := int(data.Data.TickLower)
	newTickUpper := int(data.Data.TickUpper)
	newTicks := []int{newTickLower, newTickUpper}

	for newTick := range newTicks {
		for i := range initializedTicks {
			if i == len(initializedTicks)-1 {
				if newTick > initializedTicks[i] {
					initializedTicks = append(initializedTicks, newTick)
				}

				break
			}

			if newTick > initializedTicks[i] && newTick < initializedTicks[i+1] {
				initializedTicks = append(initializedTicks, initializedTicks[:i+1]...)
				initializedTicks = append(initializedTicks, newTick)
				initializedTicks = append(initializedTicks, initializedTicks[i+1:]...)
				break
			}

		}

	}

	if newTickLower < pool.Tick && pool.Tick < newTickUpper {
		token0, ok0 := s.tokensMap[models.TokenIdentificator{Address: pool.Token0, ChainID: s.config.ChainID}]
		token1, ok1 := s.tokensMap[models.TokenIdentificator{Address: pool.Token1, ChainID: s.config.ChainID}]
		if !ok0 || !ok1 {
			return errors.New("pool tokens not found")
		}
		err := v3poolexchangable.UpdateRateFor10USD(&pool, token0, token1)
		if err != nil {
			pool.Zfo10USDRate = big.NewFloat(0)
			pool.NonZfo10USDRate = big.NewFloat(0)
			pool.IsDusty = true
		}

		pool.Liquidity.Add(pool.Liquidity, data.Data.Amount)
	}

	err = s.v3PoolDBRepo.UpdatePoolColumns(pool, []string{
		models.UNISWAP_V3_POOL_LIQUIDITY,
		models.UNISWAP_V3_NON_ZFO_10USD_RATE,
		models.UNISWAP_V3_ZFO_10USD_RATE,
		models.UNISWAP_V3_POOL_BLOCK_NUMBER,
		models.UNISWAP_V3_POOL_IS_DUSTY,
	})

	if err != nil {
		return err
	}

	return nil
}
