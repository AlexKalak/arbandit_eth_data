package merger

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/alexkalak/go_market_analyze/common/models"
)

func (m *merger) MergePools(chainID uint) error {
	db, err := m.database.GetDB()
	if err != nil {
		return err
	}

	tokens, err := m.tokenRepo.GetTokensByChainID(chainID)
	if err != nil {
		return err
	}

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
		models.UNISWAP_V3_POOL_TOKEN0,
		models.UNISWAP_V3_POOL_TOKEN1,
		models.UNISWAP_V3_POOL_SQRTPRICEX96,
		models.UNISWAP_V3_POOL_LIQUIDITY,

		models.UNISWAP_V3_POOL_TICK,
		models.UNISWAP_V3_POOL_TICK_SPACING,
		models.UNISWAP_V3_POOL_TICK_LOWER,
		models.UNISWAP_V3_POOL_TICK_UPPER,
		models.UNISWAP_V3_POOL_NEAR_TICKS,

		models.UNISWAP_V3_POOL_FEE_TIER,
		models.UNISWAP_V3_POOL_IS_DUSTY,
		models.UNISWAP_V3_POOL_BLOCK_NUMBER,

		models.UNISWAP_V3_POOL_TOKEN0_HOLDING,
		models.UNISWAP_V3_POOL_TOKEN1_HOLDING,
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
			defaultToken0Holding,
			defaultToken1Holding,
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
				models.UNISWAP_V3_POOL_TOKEN0,
				models.UNISWAP_V3_POOL_TOKEN1,
				models.UNISWAP_V3_POOL_SQRTPRICEX96,
				models.UNISWAP_V3_POOL_LIQUIDITY,

				models.UNISWAP_V3_POOL_TICK,
				models.UNISWAP_V3_POOL_TICK_SPACING,
				models.UNISWAP_V3_POOL_TICK_LOWER,
				models.UNISWAP_V3_POOL_TICK_UPPER,
				models.UNISWAP_V3_POOL_NEAR_TICKS,

				models.UNISWAP_V3_POOL_FEE_TIER,
				models.UNISWAP_V3_POOL_IS_DUSTY,
				models.UNISWAP_V3_POOL_BLOCK_NUMBER,

				models.UNISWAP_V3_POOL_TOKEN0_HOLDING,
				models.UNISWAP_V3_POOL_TOKEN1_HOLDING,
			)
		}
	}

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
