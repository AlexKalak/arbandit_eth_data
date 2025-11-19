package poolupdaterservice

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/alexkalak/go_market_analyze/common/core/exchangables/v3poolexchangable"
	"github.com/alexkalak/go_market_analyze/common/helpers"
	"github.com/alexkalak/go_market_analyze/common/models"
	"github.com/ethereum/go-ethereum/common"
	"github.com/segmentio/kafka-go"
)

type poolEventMetaData struct {
	Type        string `json:"type"`
	BlockNumber uint64 `json:"block_number"`
	Address     string `json:"address"`
	TxHash      string `json:"tx_hash"`
	TxTimestamp uint64 `json:"tx_timestamp"`
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
		GroupID: "ssanina2281337",
	})

	defer reader.Close()

	lastTimeLogged := time.Now()
	msgCount := 0

	fmt.Println("🚀 Listening for messages...")
	for {
		m, err := reader.ReadMessage(ctx)
		if err != nil {
			fmt.Println("failed to read message:", err)
			continue
		}

		msgCount += 1
		if time.Since(lastTimeLogged) > time.Second {
			fmt.Println(msgCount, "messages")
			msgCount = 0
			lastTimeLogged = time.Now()
		}

		err = s.handlePoolEventMessageForCache(&m)
		if err != nil {
			continue
		}

		chanel <- &m
	}
}

func (s *poolUpdaterService) handlePoolEventMessageForCache(m *kafka.Message) error {
	// fmt.Println("handling message in cache")
	if m == nil {
		return errors.New("nil message")
	}

	metaData := poolEventMetaData{}
	if err := json.Unmarshal(m.Value, &metaData); err != nil {
		return err
	}
	fmt.Println(metaData.Type)

	if metaData.Type == "BlockOver" {
		// fmt.Println("BLOCK OVER: ", metaData.BlockNumber)
		// fmt.Println(s.currentCheckingBlock, metaData.BlockNumber)

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
		s.handleSwapEventForCache(metaData, m)
	case "Mint":
		s.handleMintEventForCache(metaData, m)
	case "Burn":
		s.handleBurnEventForCache(metaData, m)
	default:
		// return errors.New("message type not found")
	}

	return nil
}

func (s *poolUpdaterService) handleSwapEventForCache(metaData poolEventMetaData, m *kafka.Message) error {
	fmt.Println("Swap for cache", metaData.Address)
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

	token0, ok0 := s.tokensMapForCache[models.TokenIdentificator{Address: pool.Token0, ChainID: s.config.ChainID}]
	token1, ok1 := s.tokensMapForCache[models.TokenIdentificator{Address: pool.Token1, ChainID: s.config.ChainID}]
	if !ok0 || !ok1 {
		return errors.New("pool tokens not found")
	}

	err := s.updatePoolForSwapEvent(&pool, data, token0, token1)
	if err != nil {
		return err
	}

	err = s.updateImpactsForCache(&pool, token0, token1)
	if err != nil {
		return err
	}

	s.currentBlockPoolChanges[poolIdentificator] = pool

	v3Swap := models.V3Swap{
		TxHash:                metaData.TxHash,
		TxTimestamp:           metaData.TxTimestamp,
		PoolAddress:           metaData.Address,
		ChainID:               s.config.ChainID,
		BlockNumber:           metaData.BlockNumber,
		Amount0:               data.Data.Amount0,
		Amount1:               data.Data.Amount1,
		ArchiveToken0USDPrice: token0.USDPrice,
		ArchiveToken1USDPrice: token1.USDPrice,
	}

	return s.v3TransactionCacheRepo.StreamSwap(v3Swap)

}

func (s *poolUpdaterService) handleMintEventForCache(metaData poolEventMetaData, m *kafka.Message) error {
	fmt.Println("Mint evet")
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

	err := s.handleTicksForMintBurn(&pool, int(data.Data.TickLower), int(data.Data.TickUpper), data.Data.Amount)
	if err != nil {
		return err
	}

	s.currentBlockPoolChanges[poolIdentificator] = pool

	return err
}

func (s *poolUpdaterService) handleBurnEventForCache(metaData poolEventMetaData, m *kafka.Message) error {
	fmt.Println("Burn evet")
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

	err := s.handleTicksForMintBurn(&pool, int(data.Data.TickLower), int(data.Data.TickUpper), data.Data.Amount)
	if err != nil {
		return err
	}

	s.currentBlockPoolChanges[poolIdentificator] = pool

	return err
}

func (s *poolUpdaterService) updateImpactsForCache(pool *models.UniswapV3Pool, token0, token1 *models.Token) error {
	updatedImpact, err := s.updateTokensImpactsForV3Swap(pool, token0, token1)
	if err != nil {
		return err
	}

	if updatedImpact != nil {
		switch updatedImpact.TokenAddress {
		case token0.Address:
			// fmt.Println("Updated cache impact for token0: ", token0.Symbol)
			s.tokenCacheRepo.SetToken(token0)
		case token1.Address:
			// fmt.Println("Updated cache impact for token1: ", token1.Symbol)
			s.tokenCacheRepo.SetToken(token1)
		}
	}

	return err
}

func (s *poolUpdaterService) updatePoolForSwapEvent(pool *models.UniswapV3Pool, data poolEventData[swapEventData], token0 *models.Token, token1 *models.Token) error {
	newTick := int(data.Data.Tick.Int64())
	pool.Tick = newTick
	if pool.Tick < pool.TickLower || pool.Tick > pool.TickUpper {
		pool.UpdateTickLowerUpper()
	}

	oldZfo := new(big.Float)
	oldZfo.Set(pool.Zfo10USDRate)

	pool.Liquidity = data.Data.Liquidity
	pool.SqrtPriceX96 = data.Data.SqrtPriceX96

	err := v3poolexchangable.UpdateRateFor10USD(pool, token0, token1)
	if err != nil {
		pool.Zfo10USDRate = big.NewFloat(0)
		pool.NonZfo10USDRate = big.NewFloat(0)
		pool.IsDusty = true
	}

	return nil
}

func (s *poolUpdaterService) handleTicksForMintBurn(pool *models.UniswapV3Pool, tickLower, tickUpper int, amount *big.Int) error {
	positionLowerTick := models.UniswapV3PoolTick{
		TickIdx:      tickLower,
		LiquidityNet: amount,
	}
	positionUpperTick := models.UniswapV3PoolTick{
		TickIdx:      tickUpper,
		LiquidityNet: new(big.Int).Neg(amount),
	}
	newTicks := []models.UniswapV3PoolTick{positionLowerTick, positionUpperTick}

	s.addNewTicksForPool(pool, newTicks)

	if positionLowerTick.TickIdx < pool.Tick && pool.Tick < positionUpperTick.TickIdx {
		pool.Liquidity.Add(pool.Liquidity, amount)
		err := s.updatePoolRates(pool)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *poolUpdaterService) addNewTicksForPool(pool *models.UniswapV3Pool, newTicks []models.UniswapV3PoolTick) {
	initializedTicks := pool.GetTicks()
	updatedInitializedTicks := []models.UniswapV3PoolTick{}

	fmt.Println("prevTicks: ", len(initializedTicks))

	for _, newTick := range newTicks {
		for i := range initializedTicks {
			if newTick.TickIdx == initializedTicks[i].TickIdx {
				initializedTicks[i].LiquidityNet.Add(initializedTicks[i].LiquidityNet, newTick.LiquidityNet)
				break
			}

			if i == len(initializedTicks)-1 {
				if newTick.TickIdx > initializedTicks[i].TickIdx {
					initializedTicks = append(
						initializedTicks,
						newTick,
					)
				}

				break
			}

			if newTick.TickIdx > initializedTicks[i].TickIdx && newTick.TickIdx < initializedTicks[i+1].TickIdx {
				updatedInitializedTicks = append(initializedTicks[:i+1], newTick)
				updatedInitializedTicks = append(updatedInitializedTicks, initializedTicks[i+1:]...)
				break
			}

		}
	}

	fmt.Println("newTicks: ", len(updatedInitializedTicks))

	pool.SetTicks(initializedTicks)
}

////DATABASE HANDLING

func (s *poolUpdaterService) startPostgresUpdater(ctx context.Context, chanel <-chan *kafka.Message) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case m := <-chanel:
			err := s.handlePoolEventMessageForPostgres(m)
			if err != nil {
				// fmt.Println("error handling postgres message: ", err)
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

	token0, ok0 := s.tokensMapForDB[models.TokenIdentificator{Address: pool.Token0, ChainID: s.config.ChainID}]
	token1, ok1 := s.tokensMapForDB[models.TokenIdentificator{Address: pool.Token1, ChainID: s.config.ChainID}]

	if !ok0 || !ok1 {
		return errors.New("pool tokens not found")
	}

	err = s.updatePoolForSwapEvent(&pool, data, token0, token1)
	if err != nil {
		return err
	}
	err = s.updateImpactsForDB(&pool, token0, token1)
	if err != nil {
		return err
	}

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

	v3Swap := models.V3Swap{
		TxHash:                metaData.TxHash,
		TxTimestamp:           metaData.TxTimestamp,
		PoolAddress:           metaData.Address,
		ChainID:               s.config.ChainID,
		BlockNumber:           metaData.BlockNumber,
		Amount0:               data.Data.Amount0,
		Amount1:               data.Data.Amount1,
		ArchiveToken0USDPrice: token0.USDPrice,
		ArchiveToken1USDPrice: token1.USDPrice,
	}
	fmt.Println("TXTIMESTAMP: ", v3Swap.TxTimestamp)

	err = s.v3TransactionDBRepo.CreateSwap(&v3Swap)
	if err != nil {
		fmt.Println("error creating tx:", err)
		return err
	}

	return nil
}

func (s *poolUpdaterService) updateImpactsForDB(pool *models.UniswapV3Pool, token0, token1 *models.Token) error {
	updatedImpact, err := s.updateTokensImpactsForV3Swap(pool, token0, token1)
	if err != nil {
		return err
	}

	if updatedImpact != nil {
		if updatedImpact.TokenAddress == token0.Address {
			err = s.tokenDBRepo.UpdateTokenPriceImpactAndTokenPrice(token0, updatedImpact)
		} else {
			err = s.tokenDBRepo.UpdateTokenPriceImpactAndTokenPrice(token1, updatedImpact)
		}
		if err != nil {
			fmt.Println("Unable to write update token price impact and token price")
			return err
		} else {
			fmt.Println("Updated pool impact", helpers.GetJSONString(updatedImpact))
		}
	}

	return err
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
	err = s.handleTicksForMintBurn(&pool, int(data.Data.TickLower), int(data.Data.TickUpper), data.Data.Amount)
	if err != nil {
		return err
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

	err = s.handleTicksForMintBurn(&pool, int(data.Data.TickLower), int(data.Data.TickUpper), data.Data.Amount)
	if err != nil {
		return err
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

func (s *poolUpdaterService) updatePoolRates(pool *models.UniswapV3Pool) error {
	token0, ok0 := s.tokensMapForDB[models.TokenIdentificator{Address: pool.Token0, ChainID: s.config.ChainID}]
	token1, ok1 := s.tokensMapForDB[models.TokenIdentificator{Address: pool.Token1, ChainID: s.config.ChainID}]
	if !ok0 || !ok1 {
		return errors.New("pool tokens not found")
	}
	err := v3poolexchangable.UpdateRateFor10USD(pool, token0, token1)
	if err != nil {
		pool.Zfo10USDRate = big.NewFloat(0)
		pool.NonZfo10USDRate = big.NewFloat(0)
		pool.IsDusty = true
	}

	return nil
}

func (s *poolUpdaterService) updateTokensImpactsForV3Swap(pool *models.UniswapV3Pool, token0, token1 *models.Token) (*models.TokenPriceImpact, error) {
	var token0CurrentPoolImpact *models.TokenPriceImpact = nil
	for _, imp := range token0.GetImpacts() {
		if imp.ChainID == pool.ChainID &&
			imp.TokenAddress == pool.Token0 &&
			imp.ExchangeIdentifier == models.GetExchangeIdentifierForV3Pool(
				pool.ChainID,
				pool.Address,
			) {
			token0CurrentPoolImpact = imp
			break
		}
	}

	var token1CurrentPoolImpact *models.TokenPriceImpact = nil
	for _, imp := range token1.GetImpacts() {
		if imp.ChainID == pool.ChainID &&
			imp.TokenAddress == pool.Token1 &&
			imp.ExchangeIdentifier == models.GetExchangeIdentifierForV3Pool(
				pool.ChainID,
				pool.Address,
			) {
			token1CurrentPoolImpact = imp
			break
		}
	}

	var updatedImpact *models.TokenPriceImpact

	if token0CurrentPoolImpact != nil {
		// fmt.Println("Updating impact for token0...", token0.Symbol)
		// fmt.Println("old value: ", token0CurrentPoolImpact.USDPrice)
		tokenRate, err := v3poolexchangable.GetRateForPool(pool, pool.Token0, token1.Decimals, token0.Decimals)
		if err != nil {
			// fmt.Println("unable to get price: ", err)
			return nil, nil
		}

		token0USDPrice := new(big.Float).Mul(tokenRate, token1.USDPrice)
		if token0USDPrice.Cmp(big.NewFloat(0)) == 0 {
			return nil, nil
		}

		token0CurrentPoolImpact.USDPrice = token0USDPrice
		token0CurrentPoolImpact.Impact = v3poolexchangable.GetImpactForPool(pool, int64(token1.Decimals), token1.USDPrice)

		token0.USDPrice, err = token0.AveragePrice()
		if err != nil {
			return nil, err
		}
		updatedImpact = token0CurrentPoolImpact

		// fmt.Println("new value: ", token0CurrentPoolImpact.USDPrice)
		// fmt.Println("")

	} else if token1CurrentPoolImpact != nil {
		// fmt.Println("Updating impact for token1...", token1.Symbol)
		// fmt.Println("old value: ", token1CurrentPoolImpact.USDPrice)
		tokenRate, err := v3poolexchangable.GetRateForPool(pool, pool.Token1, token0.Decimals, token1.Decimals)
		if err != nil {
			// fmt.Println("unable to get price: ", err)
			return nil, nil
		}

		token1USDPrice := new(big.Float).Mul(tokenRate, token0.USDPrice)
		if token1USDPrice.Cmp(big.NewFloat(0)) == 0 {
			return nil, nil
		}

		token1CurrentPoolImpact.USDPrice = token1USDPrice
		token1CurrentPoolImpact.Impact = v3poolexchangable.GetImpactForPool(pool, int64(token0.Decimals), token0.USDPrice)

		token1.USDPrice, err = token1.AveragePrice()
		if err != nil {
			return nil, err
		}

		updatedImpact = token1CurrentPoolImpact

		// fmt.Println("new value: ", token1CurrentPoolImpact.USDPrice)
		// fmt.Println("")
	}

	return updatedImpact, nil

}
