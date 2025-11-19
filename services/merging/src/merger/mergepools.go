package merger

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"math/big"

	"github.com/alexkalak/go_market_analyze/common/core/exchangables/v3poolexchangable"
	"github.com/alexkalak/go_market_analyze/common/helpers"
	"github.com/alexkalak/go_market_analyze/common/models"
)

func (m *merger) SortPoolTicks() error {
	pool, err := m.v3PoolsDBRepo.GetPoolByPoolIdentificator(models.V3PoolIdentificator{
		ChainID: 1,
		Address: "0x16588709ca8f7b84829b43cc1c5cb7e84a321b16",
	})

	if err != nil {
		return err
	}

	fmt.Println("Ticks before")
	for _, tick := range pool.GetTicks() {
		fmt.Println(tick.TickIdx)
	}
	pool.SortTicks()
	fmt.Println("Ticks after")
	for _, tick := range pool.GetTicks() {
		fmt.Println(tick.TickIdx)
	}

	return nil
}

func (m *merger) ImitateSwapForPool(identificator models.V3PoolIdentificator, amountUSD *big.Int) error {
	pool, err := m.v3PoolsDBRepo.GetPoolByPoolIdentificator(identificator)
	if err != nil {
		return err
	}

	token0, err := m.tokenRepo.GetTokenByIdentificator(models.TokenIdentificator{
		Address: pool.Token0,
		ChainID: identificator.ChainID,
	})
	if err != nil {
		return err
	}

	token1, err := m.tokenRepo.GetTokenByIdentificator(models.TokenIdentificator{
		Address: pool.Token1,
		ChainID: identificator.ChainID,
	})
	if err != nil {
		return err
	}

	ex, err := v3poolexchangable.NewV3ExchangablePool(&pool, token0, token1)
	if err != nil {
		return err
	}

	zfoIn := token0.FromUSD(amountUSD)
	fmt.Println("zfoIn: ", zfoIn.String())
	zfoOut, err := ex.ImitateSwap(zfoIn, true)
	if err != nil {
		fmt.Println("ZFO ERR: ", err)
		return err
	}
	fmt.Println("zfoOut: ", zfoOut.String())

	nonZfoIn := token1.FromUSD(amountUSD)
	fmt.Println("nonZfoIn: ", nonZfoIn.String())
	nonZfoOut, err := ex.ImitateSwap(nonZfoIn, false)
	if err != nil {
		fmt.Println("NON ZFO ERR: ", err)
		return err
	}
	fmt.Println("nonZfoOut: ", nonZfoOut.String())

	fmt.Println("ZFO:", zfoIn, "->", zfoOut)
	fmt.Println("NON ZFO:", nonZfoIn, "->", nonZfoOut)

	return nil
}

func (m *merger) MergePools(chainID uint) error {
	err := m.v3PoolsDBRepo.DeletePoolsByChain(chainID)
	if err != nil {
		fmt.Println("Deleting pools error: ", err)
		return err
	}

	db, err := m.database.GetDB()
	if err != nil {
		return err
	}

	tokens, err := m.tokenRepo.GetTokensByChainID(chainID)
	if err != nil {
		return err
	}
	fmt.Println("Len tokens: ", len(tokens))

	pools, err := m.subgraphClient.GetV3Pools(context.Background(), chainID)
	if err != nil {
		return err
	}

	tokensMap := map[string]any{}
	for _, token := range tokens {
		tokensMap[token.Address] = new(any)
	}

	query := psql.Insert(models.UNISWAP_V3_POOL_TABLE).Columns(
		models.UNISWAP_V3_POOL_ADDRESS,
		models.UNISWAP_V3_POOL_CHAINID,
		models.UNISWAP_V3_POOL_EXCHANGE_NAME,
		models.UNISWAP_V3_POOL_TOKEN0_ADDRESS,
		models.UNISWAP_V3_POOL_TOKEN1_ADDRESS,
		models.UNISWAP_V3_POOL_SQRTPRICEX96,
		models.UNISWAP_V3_POOL_LIQUIDITY,

		models.UNISWAP_V3_POOL_TICK,
		models.UNISWAP_V3_POOL_TICK_SPACING,
		models.UNISWAP_V3_POOL_TICK_LOWER,
		models.UNISWAP_V3_POOL_TICK_UPPER,
		models.UNISWAP_V3_POOL_TICKS,

		models.UNISWAP_V3_POOL_FEE_TIER,
		models.UNISWAP_V3_POOL_IS_DUSTY,
		models.UNISWAP_V3_POOL_BLOCK_NUMBER,

		models.UNISWAP_V3_ZFO_10USD_RATE,
		models.UNISWAP_V3_NON_ZFO_10USD_RATE,
	)

	poolsMap := map[string]any{}

	badPools := 0
	for i, pool := range pools {
		_, ok1 := tokensMap[pool.Token0]
		_, ok2 := tokensMap[pool.Token1]
		if !ok1 || !ok2 {
			badPools++
			continue
		}

		if _, ok := poolsMap[pool.Address]; ok {
			fmt.Println("found duplicate")
			continue
		}

		poolsMap[pool.Address] = new(any)

		query = query.Values(
			pool.Address,
			chainID,
			pool.ExchangeName,
			pool.Token0,
			pool.Token1,
			defaultSqrtPriceX96,
			defaultLiquidity,
			defaultTick,
			defaultTickSpacing,
			defaultTickLower,
			defaultTickUpper,
			defaultNearTicks,
			pool.FeeTier,
			defaultIsDusty,
			defaultBlockNumber,

			defaultZfo10USDRate,
			defaultNonZfo10USDRate,
		)

		if (i+1)%2000 == 0 {
			resp, err := query.RunWith(db).Exec()
			if err != nil {
				return err
			}

			rowsAff, err := resp.RowsAffected()
			if err != nil {
				return err
			}
			fmt.Println("Pools inserted", rowsAff)

			query = psql.Insert(models.UNISWAP_V3_POOL_TABLE).Columns(
				models.UNISWAP_V3_POOL_ADDRESS,
				models.UNISWAP_V3_POOL_CHAINID,
				models.UNISWAP_V3_POOL_EXCHANGE_NAME,
				models.UNISWAP_V3_POOL_TOKEN0_ADDRESS,
				models.UNISWAP_V3_POOL_TOKEN1_ADDRESS,
				models.UNISWAP_V3_POOL_SQRTPRICEX96,
				models.UNISWAP_V3_POOL_LIQUIDITY,

				models.UNISWAP_V3_POOL_TICK,
				models.UNISWAP_V3_POOL_TICK_SPACING,
				models.UNISWAP_V3_POOL_TICK_LOWER,
				models.UNISWAP_V3_POOL_TICK_UPPER,
				models.UNISWAP_V3_POOL_TICKS,

				models.UNISWAP_V3_POOL_FEE_TIER,
				models.UNISWAP_V3_POOL_IS_DUSTY,
				models.UNISWAP_V3_POOL_BLOCK_NUMBER,

				models.UNISWAP_V3_ZFO_10USD_RATE,
				models.UNISWAP_V3_NON_ZFO_10USD_RATE,
			)
		}
	}

	fmt.Println("Bad pools: ", badPools)

	resp, err := query.RunWith(db).Exec()
	if err != nil && err != sql.ErrNoRows {
		return err
	}

	rowsAff, err := resp.RowsAffected()
	if err != nil {
		return err
	}
	fmt.Println("End pools inserted", rowsAff)

	return nil
}

func (m *merger) MergePoolsData(ctx context.Context, chainID uint, blockNumber *big.Int) error {
	pools, err := m.v3PoolsDBRepo.GetPoolsByChainIDWith0BlockNumberOrdered(chainID)
	if err != nil {
		return err
	}
	fmt.Println("POols: ", len(pools))
	poolsMap := map[string]*models.UniswapV3Pool{}
	for _, pool := range pools {
		poolsMap[pool.Address] = &pool
	}

	poolsForQuery := 100

	for startPoolIndex := 0; startPoolIndex < len(pools); startPoolIndex += poolsForQuery {
		fmt.Println("start pool index: ", startPoolIndex)
		slice := pools[startPoolIndex:]
		if startPoolIndex+poolsForQuery < len(pools) {
			slice = pools[startPoolIndex : startPoolIndex+poolsForQuery]
		}
		for i := range slice {
			if slice[i].Address == "0x16588709ca8f7b84829b43cc1c5cb7e84a321b16" {
				fmt.Println("in slice: ")
			}

		}

		updatedPools, err := m.rpcClient.GetPoolsData(ctx, slice, chainID, blockNumber)
		if err != nil {
			return err
		}

		for i := range updatedPools {
			if updatedPools[i].Address == "0x16588709ca8f7b84829b43cc1c5cb7e84a321b16" {
				fmt.Println("Got that shitty pool")
			}

			updatedPools[i].TickLower = 0
			updatedPools[i].TickUpper = 0

			poolFromDB, ok := poolsMap[updatedPools[i].Address]
			if !ok {
				fmt.Println("Not found wtf")
				continue
			}

			var lowerTick int64 = math.MinInt64
			var upperTick int64 = math.MaxInt64

			ticks := poolFromDB.GetTicks()
			fmt.Println("Ticks len: ", ticks)
			for _, tick := range ticks {
				if int64(tick.TickIdx) > lowerTick && tick.TickIdx < updatedPools[i].Tick {
					lowerTick = int64(tick.TickIdx)
				}
				if int64(tick.TickIdx) < upperTick && tick.TickIdx > updatedPools[i].Tick {
					upperTick = int64(tick.TickIdx)
				}
			}

			if lowerTick > math.MinInt64 && upperTick < math.MaxInt64 {
				fmt.Println("Setting ticks", updatedPools[i].Address)

				updatedPools[i].TickLower = int(lowerTick)
				updatedPools[i].TickUpper = int(upperTick)
			}
		}

		err = m.v3PoolsDBRepo.UpdatePoolsColumns(
			updatedPools,
			[]string{
				models.UNISWAP_V3_POOL_LIQUIDITY,
				models.UNISWAP_V3_POOL_SQRTPRICEX96,
				models.UNISWAP_V3_POOL_TICK,
				models.UNISWAP_V3_POOL_TICK_LOWER,
				models.UNISWAP_V3_POOL_TICK_UPPER,
				models.UNISWAP_V3_POOL_TICK_SPACING,
				models.UNISWAP_V3_POOL_BLOCK_NUMBER,
			},
		)

		if err != nil {
			fmt.Println("Error updating database", err)
			return err
		}

	}

	return nil
}

func (m *merger) MergePoolsTicks(ctx context.Context, chainID uint) error {
	pools, err := m.v3PoolsDBRepo.GetPoolsByChainIDOrdered(chainID)
	if err != nil {
		return err
	}
	// poolsForQuery := 1000
	// lenPools := len(pools)

	poolsMap := map[string]*models.UniswapV3Pool{}
	for _, pool := range pools {
		poolsMap[pool.Address] = &pool
	}

	poolsTicks, err := m.subgraphClient.GetTicksForV3Pools(ctx, chainID, poolsMap)
	if err != nil {
		return err
	}

	updatingPools := []models.UniswapV3Pool{}
	for _, pool := range pools {
		ticks, ok := poolsTicks[pool.Address]
		if !ok {
			continue
		}
		pool.SetTicks(ticks)
		updatingPools = append(updatingPools, pool)
	}

	err = m.v3PoolsDBRepo.UpdatePoolsColumns(updatingPools, []string{
		models.UNISWAP_V3_POOL_TICKS,
	})

	if err != nil {
		return err
	}

	return nil
}

func (m *merger) MergePoolsTicksFromRPC(ctx context.Context, chainID uint, blockNumber *big.Int) error {
	pools, err := m.v3PoolsDBRepo.GetPoolsByChainIDOrdered(chainID)
	if err != nil {
		return err
	}
	poolsForQuery := 1000
	lenPools := len(pools)

	for startPoolIndex := 0; startPoolIndex < lenPools; startPoolIndex += poolsForQuery {
		fmt.Println("start pool index: ", startPoolIndex)

		slice := pools[startPoolIndex:lenPools]
		if startPoolIndex+poolsForQuery < lenPools {
			slice = pools[startPoolIndex : startPoolIndex+poolsForQuery]
		}
		fmt.Println("slice len: ", len(slice))

		updatedPools, err := m.rpcClient.GetPoolsTicks(ctx, slice, chainID, blockNumber)
		if err != nil {
			return err
		}

		fmt.Println("UpdatedPools", updatedPools)

		err = m.v3PoolsDBRepo.UpdatePoolsLowerUpperNearTicks(
			updatedPools,
		)

		if err != nil {
			fmt.Println("Error updating database", err)
			return err
		}

	}

	return nil
}

func (m *merger) ValidateV3PoolsAndComputeAverageUSDPrice(chainID uint) error {
	stableCoins, err := m.tokenRepo.GetTokensByAddressesAndChainID(USD_STABLECOIN_ADDRESSES, chainID)
	if err != nil {
		return err
	}
	fmt.Println(helpers.GetJSONString(stableCoins))

	pools, err := m.v3PoolsDBRepo.GetPoolsByChainID(chainID)
	if err != nil {
		return err
	}

	err = m.tokenRepo.DeleteV3PoolImpacts(chainID)
	if err != nil {
		return err
	}

	tokens, err := m.tokenRepo.GetTokensByChainID(chainID)
	if err != nil {
		return err
	}
	fmt.Println("Len tokens: ", len(tokens))

	definedTokens, notDustyPoolsMap, err := v3poolexchangable.
		ValidateV3PoolsAndGetAverageUSDPriceForTokens(
			chainID,
			tokens,
			pools,
			stableCoins,
		)

	if err != nil {
		return err
	}

	fmt.Println("Not dusty pools: ", len(notDustyPoolsMap))
	lenImpacts := 0
	for _, token := range definedTokens {
		lenImpacts += len(token.GetImpacts())
	}

	err = m.tokenRepo.UpdateTokens(definedTokens)
	if err != nil {
		return err
	}

	notDustyPools := make([]models.UniswapV3Pool, 0, len(notDustyPoolsMap))
	for i, _ := range pools {
		if nonDustyPool, ok := notDustyPoolsMap[pools[i].Address]; ok {
			pools[i].IsDusty = false

			pools[i].Zfo10USDRate = nonDustyPool.Zfo10USDRate
			pools[i].NonZfo10USDRate = nonDustyPool.NonZfo10USDRate

			notDustyPools = append(notDustyPools, pools[i])
		}
	}

	err = m.v3PoolsDBRepo.SetAllPoolsToDusty(chainID)
	if err != nil {
		return err
	}

	err = m.v3PoolsDBRepo.UpdatePoolsIsDusty(notDustyPools)
	if err != nil {
		return err
	}

	return nil
}
