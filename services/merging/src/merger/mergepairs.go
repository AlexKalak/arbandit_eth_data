package merger

import (
	"context"
	"database/sql"
	"fmt"
	"math/big"

	"github.com/alexkalak/go_market_analyze/common/core/exchangables/v2pairexchangable"
	"github.com/alexkalak/go_market_analyze/common/helpers"
	"github.com/alexkalak/go_market_analyze/common/models"
)

func (m *merger) MergePairs(chainID uint) error {
	err := m.v2PairDBRepo.DeletePairsByChain(chainID)
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

	pairs, err := m.subgraphClient.GetV2Pairs(context.Background(), chainID)
	if err != nil {
		return err
	}

	tokensMap := map[string]any{}
	for _, token := range tokens {
		tokensMap[token.Address] = new(any)
	}

	query := psql.Insert(models.UNISWAP_V2_PAIR_TABLE).Columns(
		models.UNISWAP_V2_PAIR_ADDRESS,
		models.UNISWAP_V2_PAIR_CHAINID,
		models.UNISWAP_V2_PAIR_EXCHANGE_NAME,
		models.UNISWAP_V2_PAIR_TOKEN0_ADDRESS,
		models.UNISWAP_V2_PAIR_TOKEN1_ADDRESS,
		models.UNISWAP_V2_PAIR_AMOUNT0,
		models.UNISWAP_V2_PAIR_AMOUNT1,
		models.UNISWAP_V2_PAIR_FEE_TIER,
		models.UNISWAP_V2_PAIR_IS_DUSTY,
		models.UNISWAP_V2_PAIR_BLOCK_NUMBER,
		models.UNISWAP_V2_ZFO_10USD_RATE,
		models.UNISWAP_V2_NON_ZFO_10USD_RATE,
	)

	pairsMap := map[string]any{}

	badPairs := 0
	for i, pair := range pairs {
		_, ok1 := tokensMap[pair.Token0]
		_, ok2 := tokensMap[pair.Token1]
		if !ok1 || !ok2 {
			badPairs++
			continue
		}

		if _, ok := pairsMap[pair.Address]; ok {
			fmt.Println("found duplicate")
			continue
		}

		pairsMap[pair.Address] = new(any)

		query = query.Values(
			pair.Address,
			chainID,
			pair.ExchangeName,
			pair.Token0,
			pair.Token1,
			defaultAmount0,
			defaultAmount1,
			pair.FeeTier,
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
			fmt.Println("Pairs inserted", rowsAff)

			query = psql.Insert(models.UNISWAP_V2_PAIR_TABLE).Columns(
				models.UNISWAP_V2_PAIR_ADDRESS,
				models.UNISWAP_V2_PAIR_CHAINID,
				models.UNISWAP_V2_PAIR_EXCHANGE_NAME,
				models.UNISWAP_V2_PAIR_TOKEN0_ADDRESS,
				models.UNISWAP_V2_PAIR_TOKEN1_ADDRESS,
				models.UNISWAP_V2_PAIR_AMOUNT0,
				models.UNISWAP_V2_PAIR_AMOUNT1,
				models.UNISWAP_V2_PAIR_FEE_TIER,
				models.UNISWAP_V2_PAIR_IS_DUSTY,
				models.UNISWAP_V2_PAIR_BLOCK_NUMBER,
				models.UNISWAP_V2_ZFO_10USD_RATE,
				models.UNISWAP_V2_NON_ZFO_10USD_RATE,
			)
		}
	}

	fmt.Println("Bad pools: ", badPairs)

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

func (m *merger) MergePairsData(ctx context.Context, chainID uint, blockNumber *big.Int) error {
	pairs, err := m.v2PairDBRepo.GetPairsByChainID(chainID)
	if err != nil {
		return err
	}
	fmt.Println("Pairs: ", len(pairs))
	pairsMap := map[string]*models.UniswapV2Pair{}
	for _, pair := range pairs {
		pairsMap[pair.Address] = &pair
	}

	pairsForQuery := 100

	for startPairIndex := 0; startPairIndex < len(pairs); startPairIndex += pairsForQuery {
		fmt.Println("start pool index: ", startPairIndex)
		slice := pairs[startPairIndex:]
		if startPairIndex+pairsForQuery < len(pairs) {
			slice = pairs[startPairIndex : startPairIndex+pairsForQuery]
		}

		updatedPairs, err := m.rpcClient.GetPairsData(ctx, slice, chainID, blockNumber)
		if err != nil {
			return err
		}

		err = m.v2PairDBRepo.UpdatePairsColumns(
			updatedPairs,
			[]string{
				models.UNISWAP_V2_PAIR_AMOUNT0,
				models.UNISWAP_V2_PAIR_AMOUNT1,
				models.UNISWAP_V2_PAIR_BLOCK_NUMBER,
			},
		)

		if err != nil {
			fmt.Println("Error updating database", err)
			return err
		}

	}

	return nil
}

func (m *merger) ValidateV2PairsAndComputeAverageUSDPrice(chainID uint) error {
	stableCoins, err := m.tokenRepo.GetTokensByAddressesAndChainID(USD_STABLECOIN_ADDRESSES, chainID)
	if err != nil {
		return err
	}
	fmt.Println(helpers.GetJSONString(stableCoins))

	pairs, err := m.v2PairDBRepo.GetPairsByChainID(chainID)
	if err != nil {
		return err
	}

	err = m.tokenRepo.DeleteV2PairImpacts(chainID)
	if err != nil {
		return err
	}

	tokens, err := m.tokenRepo.GetTokensByChainID(chainID)
	if err != nil {
		return err
	}
	fmt.Println("Len tokens: ", len(tokens))

	definedTokens, notDustyPairsMap, err := v2pairexchangable.
		ValidateV2PairsAndGetAverageUSDPriceForTokens(
			chainID,
			tokens,
			pairs,
			stableCoins,
		)

	if err != nil {
		return err
	}

	fmt.Println("Not dusty pools: ", len(notDustyPairsMap))
	lenImpacts := 0
	for _, token := range definedTokens {
		lenImpacts += len(token.GetImpacts())
	}

	err = m.tokenRepo.UpdateTokens(definedTokens)
	if err != nil {
		return err
	}

	notDustyPairs := make([]models.UniswapV2Pair, 0, len(notDustyPairsMap))
	for i, _ := range pairs {
		if nonDustyPair, ok := notDustyPairsMap[pairs[i].Address]; ok {
			pairs[i].IsDusty = false

			pairs[i].Zfo10USDRate = nonDustyPair.Zfo10USDRate
			pairs[i].NonZfo10USDRate = nonDustyPair.NonZfo10USDRate

			notDustyPairs = append(notDustyPairs, pairs[i])
		}
	}

	err = m.v2PairDBRepo.SetAllPairsToDusty(chainID)
	if err != nil {
		return err
	}

	err = m.v2PairDBRepo.UpdatePairsColumns(notDustyPairs, []string{
		models.UNISWAP_V2_PAIR_IS_DUSTY,
		models.UNISWAP_V2_ZFO_10USD_RATE,
		models.UNISWAP_V2_NON_ZFO_10USD_RATE,
	})
	if err != nil {
		return err
	}

	return nil
}
