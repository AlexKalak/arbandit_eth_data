package subgraphs

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/alexkalak/go_market_analyze/common/external/subgraphs/subgrapherrors"
	"github.com/alexkalak/go_market_analyze/common/models"
	"github.com/machinebox/graphql"
)

type ExchangeType string
type ExchangeName string

const (
	uniV2Fork ExchangeType = "uni_v2_fork"
	uniV3Fork ExchangeType = "uni_v3_fork"
)

//go:embed subgraphassets/subgraphurls.json
var subgraphUrlsMapString string

type subgraphUrlsMap map[uint]map[ExchangeType]map[ExchangeName]string

type SubgraphClient interface {
	GetTokensForV3Contract(ctx context.Context, chainID uint) ([]models.Token, error)
	GetV3Pools(ctx context.Context, chainID uint) ([]models.UniswapV3Pool, error)
	GetTicksForV3Pools(ctx context.Context, chainID uint, poolsMap map[string]*models.UniswapV3Pool) (map[string][]models.UniswapV3PoolTick, error)
}

type SubgraphClientConfig struct {
	APIKey string
}
type subgraphClient struct {
	subgraphUrlsMap subgraphUrlsMap
	apiKey          string
}

func NewSubgraphClient(config SubgraphClientConfig) (SubgraphClient, error) {
	subgraphUrlsMap := subgraphUrlsMap{}

	err := json.Unmarshal([]byte(subgraphUrlsMapString), &subgraphUrlsMap)
	if err != nil {
		return nil, errors.New("unable to parse subgraph urls map")
	}

	return &subgraphClient{
		subgraphUrlsMap: subgraphUrlsMap,
		apiKey:          config.APIKey,
	}, nil
}

const tokensChunkSize = 3000

func (s *subgraphClient) GetTokensForV3Contract(ctx context.Context, chainID uint) ([]models.Token, error) {
	url, ok := s.subgraphUrlsMap[chainID][uniV3Fork]["uniswap"]
	if !ok {
		return nil, subgrapherrors.ErrExchangeTypeNotFound
	}
	tokenResponsesArray := []TokenResponse{}

	currentChunk := 0
	wg := sync.WaitGroup{}
	parallelQueries := 5

	//tmp
	for {
		tokensToPush := make([]TokenResponse, parallelQueries*tokensChunkSize)
		var totalNewValue int32 = 0
		for i := range parallelQueries {
			chunk := currentChunk
			queryNumber := i
			wg.Go(func() {
				tokensArray, err := s.queryTokens(ctx, url, chunk*tokensChunkSize)
				if err != nil {
					fmt.Println(err)
					return
				}
				fmt.Println(queryNumber, "resp Toksn: ", len(tokensArray))
				for i, token := range tokensArray {
					tokensToPush[(tokensChunkSize*queryNumber)+i] = token
				}
				atomic.AddInt32(&totalNewValue, int32(len(tokensArray)))
			})
			currentChunk++
		}
		wg.Wait()
		if totalNewValue == 0 {
			break
		}

		tokenResponsesArray = append(tokenResponsesArray, tokensToPush...)
	}

	result := make([]models.Token, 0, len(tokenResponsesArray))
	for _, tokenResp := range tokenResponsesArray {
		if tokenResp.ID != "" {
			decimals, err := strconv.Atoi(tokenResp.Decimals)
			if err != nil {
				continue
			}
			result = append(result, models.Token{
				Address:  tokenResp.ID,
				ChainID:  chainID,
				Decimals: decimals,
				Name:     tokenResp.Name,
				Symbol:   tokenResp.Symbol,
			})
		}
	}

	fmt.Println("result: ", len(result))

	return result, nil
}

//go:embed subgraphassets/v3gettokensquery.graphql
var tokensQuery string

func (s *subgraphClient) queryTokens(ctx context.Context, graphURL string, skip int) ([]TokenResponse, error) {
	client := graphql.NewClient(graphURL)
	if client == nil {
		return nil, errors.New("unable to create graphql client")
	}
	req := graphql.NewRequest(tokensQuery)
	req.Header.Add("Authorization", "Bearer "+s.apiKey)

	req.Var("first", tokensChunkSize)
	req.Var("skip", skip)

	respData := struct {
		Tokens []TokenResponse `json:"tokens"`
	}{}

	if err := client.Run(ctx, req, &respData); err != nil {
		return nil, err
	}

	return respData.Tokens, nil
}

const poolsChunkSize = 1000

func (s *subgraphClient) GetV3Pools(ctx context.Context, chainID uint) ([]models.UniswapV3Pool, error) {
	urls, ok := s.subgraphUrlsMap[chainID][uniV3Fork]
	if !ok {
		return nil, subgrapherrors.ErrExchangeTypeNotFound
	}
	poolResponsesArray := []PoolResponse{}

	parallelQueries := 5
	wg := sync.WaitGroup{}

	for exchangeName, url := range urls {
		currentChunk := 0
		fmt.Println("exchangeName: ", exchangeName)
		fmt.Println(url)
		for {
			poolsToPush := make([]PoolResponse, parallelQueries*poolsChunkSize)
			var totalNewValue int32 = 0

			for i := range parallelQueries {
				chunk := currentChunk
				queryNumber := i
				wg.Go(func() {
					a := 0
					poolsArray, err := s.queryV3Pools(ctx, url, chunk*poolsChunkSize)
					if err != nil {
						fmt.Println(err)
						return
					}
					for i, pool := range poolsArray {
						if pool.ID != "" && pool.Token0.ID != "" && pool.Token1.ID != "" {
							a++
						}
						poolsToPush[(poolsChunkSize*queryNumber)+i] = pool
						poolsToPush[(poolsChunkSize*queryNumber)+i].ExchangeName = string(exchangeName)
					}

					fmt.Println(a)
					atomic.AddInt32(&totalNewValue, int32(len(poolsArray)))
				})
				currentChunk++
			}
			wg.Wait()
			fmt.Println(exchangeName, len(poolsToPush))
			if totalNewValue == 0 {
				break
			}

			poolResponsesArray = append(poolResponsesArray, poolsToPush...)

		}
	}

	fmt.Println(len(poolResponsesArray))

	result := make([]models.UniswapV3Pool, 0, len(poolResponsesArray))

	for _, poolResp := range poolResponsesArray {
		if poolResp.ID != "" && poolResp.Token0.ID != "" && poolResp.Token1.ID != "" {
			feeTier, err := strconv.Atoi(poolResp.FeeTier)
			if err != nil {
				continue
			}

			newPool := models.UniswapV3Pool{
				ExchangeName: poolResp.ExchangeName,
				Address:      poolResp.ID,
				ChainID:      chainID,
				FeeTier:      feeTier,
				Token0:       poolResp.Token0.ID,
				Token1:       poolResp.Token1.ID,
			}

			result = append(result, newPool)
		}
	}

	fmt.Println("result: ", len(result))

	return result, nil
}

const ticksChunkSize = 1000

func (s *subgraphClient) GetTicksForV3Pools(ctx context.Context, chainID uint, poolsMap map[string]*models.UniswapV3Pool) (map[string][]models.UniswapV3PoolTick, error) {
	urls, ok := s.subgraphUrlsMap[chainID][uniV3Fork]
	if !ok {
		return nil, subgrapherrors.ErrExchangeTypeNotFound
	}

	tickResponsesArray := []PoolTickResponse{}

	parallelQueries := 5

	wg := sync.WaitGroup{}

	for exchangeName, url := range urls {
		currentChunk := 0
		fmt.Println("exchangeName: ", exchangeName)
		fmt.Println(url)

		for {
			ticksToPush := make([]PoolTickResponse, parallelQueries*ticksChunkSize)
			var totalNewValue int32 = 0

			for i := range parallelQueries {
				chunk := currentChunk
				queryNumber := i
				wg.Go(func() {
					a := 0
					ticksArray, err := s.queryV3Ticks(ctx, url, chunk*ticksChunkSize)
					if err != nil {
						fmt.Println("Error returned from queryV3Ticks: ", err)
						return
					}
					for i, tick := range ticksArray {
						if tick.LiquidityNet != "" {
							a++
						}
						ticksToPush[(poolsChunkSize*queryNumber)+i] = tick
					}

					fmt.Println(a)
					atomic.AddInt32(&totalNewValue, int32(len(ticksArray)))
				})
				currentChunk++
			}
			wg.Wait()
			fmt.Println(exchangeName, len(ticksToPush))
			if totalNewValue == 0 {
				break
			}

			tickResponsesArray = append(tickResponsesArray, ticksToPush...)
		}
	}

	fmt.Println("Total Length responses", len(tickResponsesArray))

	poolTicksMap := map[string][]models.UniswapV3PoolTick{}

	badTick := 0
	notInPools := 0
	for _, tick := range tickResponsesArray {
		if tick.LiquidityNet == "" || tick.PoolAddress == "" {
			badTick++
			continue
		}

		_, ok := poolsMap[tick.PoolAddress]
		if !ok {
			notInPools++
			continue
		}

		existingTicks, ok := poolTicksMap[tick.PoolAddress]
		if !ok {
			existingTicks = []models.UniswapV3PoolTick{}
		}

		tickIdx, err := strconv.Atoi(tick.TickIdx)
		if err != nil {
			break
		}

		liqNet, ok := new(big.Int).SetString(tick.LiquidityNet, 10)
		if !ok {
			break
		}

		existingTicks = append(existingTicks, models.UniswapV3PoolTick{
			TickIdx:      tickIdx,
			LiquidityNet: liqNet,
		})

		poolTicksMap[tick.PoolAddress] = existingTicks
	}

	fmt.Println(badTick)
	fmt.Println(notInPools)

	totalGoodPools := 0
	for poolAddress, ticks := range poolTicksMap {
		if len(ticks) > 1 {

			totalGoodPools++
			fmt.Println(poolAddress, len(ticks))
		}
	}

	return poolTicksMap, nil
}

//go:embed subgraphassets/v3poolsquery.graphql
var poolsQuery string

func (s *subgraphClient) queryV3Pools(ctx context.Context, graphURL string, skip int) ([]PoolResponse, error) {
	client := graphql.NewClient(graphURL)
	if client == nil {
		return nil, errors.New("unable to create graphql client")
	}
	req := graphql.NewRequest(poolsQuery)
	req.Header.Add("Authorization", "Bearer "+s.apiKey)

	req.Var("first", poolsChunkSize)
	req.Var("skip", skip)

	respData := struct {
		Pools []PoolResponse `json:"pools"`
	}{}

	if err := client.Run(ctx, req, &respData); err != nil {
		return nil, err
	}

	return respData.Pools, nil
}

//go:embed subgraphassets/v3ticksquery.graphql
var ticksQuery string

func (s *subgraphClient) queryV3Ticks(ctx context.Context, graphURL string, skip int) ([]PoolTickResponse, error) {
	client := graphql.NewClient(graphURL)
	if client == nil {
		return nil, errors.New("unable to create graphql client")
	}
	req := graphql.NewRequest(ticksQuery)
	req.Header.Add("Authorization", "Bearer "+s.apiKey)

	req.Var("first", ticksChunkSize)
	req.Var("skip", skip)

	respData := struct {
		Ticks []PoolTickResponse `json:"ticks"`
	}{}

	if err := client.Run(ctx, req, &respData); err != nil {
		return nil, err
	}

	return respData.Ticks, nil
}
