package arbitrageservice

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"slices"
	"time"

	"github.com/alexkalak/go_market_analyze/common/core/exchangables"
	"github.com/alexkalak/go_market_analyze/common/core/exchangables/v3poolexchangable"
	"github.com/alexkalak/go_market_analyze/common/core/exchangegraph"
	"github.com/alexkalak/go_market_analyze/common/models"
	"github.com/alexkalak/go_market_analyze/common/repo/exchangerepo/v2pairsrepo"
	"github.com/alexkalak/go_market_analyze/common/repo/exchangerepo/v3poolsrepo"
	"github.com/alexkalak/go_market_analyze/common/repo/tokenrepo"
	"github.com/alexkalak/go_market_analyze/common/repo/transactionrepo/v3transactionrepo"
)

type ArbitrageService interface {
	FindOldArbs() error
	FindAllArbs() ([]Arbitrage, error)
	Start(ctx context.Context) error
}

type arbitrageService struct {
	chainID              uint
	exchangeGraph        exchangegraph.ExchangesGraph
	v3PoolDBRepo         v3poolsrepo.V3PoolDBRepo
	v3PoolCacheRepo      v3poolsrepo.V3PoolCacheRepo
	v3TransactionsDBRepo v3transactionrepo.V3TransactionDBRepo
	tokenDBRepo          tokenrepo.TokenRepo
}

type ArbitrageServiceDependencies struct {
	TokenRepo           tokenrepo.TokenRepo
	V3PoolCacheRepo     v3poolsrepo.V3PoolCacheRepo
	V2PairRepo          v2pairsrepo.V2PairRepo
	V3PoolDBRepo        v3poolsrepo.V3PoolDBRepo
	V3TransactionDBRepo v3transactionrepo.V3TransactionDBRepo
}

func (d *ArbitrageServiceDependencies) validate() error {
	if d.TokenRepo == nil {
		return errors.New("arbitrage service dependencies token repo cannot be nil")

	}
	if d.V3PoolCacheRepo == nil {
		return errors.New("arbitrage service dependencies token repo cannot be nil")
	}
	if d.V3PoolDBRepo == nil {
		return errors.New("arbitrage service dependencies V3PoolDBRepo cannot be nil")
	}
	if d.V3TransactionDBRepo == nil {
		return errors.New("arbitrage service dependencies V3TransactionRepo cannot be nil")
	}
	// if d.V2PairRepo== nil {
	// 	return errors.New("arbitrage service dependencies token repo cannot be nil")
	//
	// }
	return nil
}

func New(chainID uint, dependencies ArbitrageServiceDependencies) (ArbitrageService, error) {
	if err := dependencies.validate(); err != nil {
		return nil, err
	}

	service := &arbitrageService{
		chainID:              chainID,
		v3TransactionsDBRepo: dependencies.V3TransactionDBRepo,
		v3PoolDBRepo:         dependencies.V3PoolDBRepo,
		v3PoolCacheRepo:      dependencies.V3PoolCacheRepo,
		tokenDBRepo:          dependencies.TokenRepo,
	}
	err := service.updateGraph()
	if err != nil {
		return nil, err
	}

	return service, nil
}

type oldArb struct {
	t          int
	amountIn   *big.Int
	amountOut  *big.Int
	pathString string
	txHash     string
	tokens     []string
}

type SwapChainNode struct {
	SwapID       int
	NextSwapNode *SwapChainNode
	PrevSwapNode *SwapChainNode
}

func (s *arbitrageService) Start(ctx context.Context) error {
	ticker := time.NewTicker(time.Second)
	for {
		select {
		case <-ctx.Done():
			return errors.New("arbitrage service ctx done")
		case <-ticker.C:
			s.updateGraph()
			s.exchangeGraph.FindAllArbs(3, big.NewInt(int64(s.chainID)))
		}
	}
}

func (s *arbitrageService) updateGraph() error {
	tokens, err := s.tokenDBRepo.GetTokens()
	if err != nil {
		panic(err)
	}
	tokenIDs := map[models.TokenIdentificator]*models.Token{}
	for _, token := range tokens {
		tokenIDs[token.GetIdentificator()] = token
	}

	pools, err := s.v3PoolCacheRepo.GetNonDustyPools(s.chainID)
	if err != nil {
		panic(err)
	}

	exchangablesArray := make([]exchangables.Exchangable, 0, len(pools))

	for _, pool := range pools {
		token0, ok := tokenIDs[models.TokenIdentificator{Address: pool.Token0, ChainID: s.chainID}]
		if !ok {
			continue
		}
		token1, ok := tokenIDs[models.TokenIdentificator{Address: pool.Token1, ChainID: s.chainID}]
		if !ok {
			continue
		}

		v3Exchangable, err := v3poolexchangable.NewV3ExchangablePool(
			&pool,
			token0,
			token1,
		)
		if err != nil {
			continue
		}

		exchangablesArray = append(exchangablesArray, &v3Exchangable)
	}

	fmt.Println("Len exchangables: ", len(exchangablesArray))

	exGraphDependencies := exchangegraph.ExchangeGraphDependencies{}

	exchangeGraph, err := exchangegraph.New(exchangablesArray, exGraphDependencies)
	if err != nil {
		return err
	}

	s.exchangeGraph = exchangeGraph
	return nil
}

type ArbitragePathUnit struct {
	TokenInAddress  string
	TokenOutAddress string
	PoolAddress     string
	AmountIn        *big.Int
	AmountOut       *big.Int
}

type Arbitrage struct {
	Path []ArbitragePathUnit
}

func (s *arbitrageService) FindAllArbs() ([]Arbitrage, error) {
	err := s.updateGraph()
	if err != nil {
		return nil, err
	}

	arbs, err := s.exchangeGraph.FindAllArbs(3, big.NewInt(10))
	if err != nil {
		return nil, err
	}

	res := []Arbitrage{}

	for _, arb := range arbs {
		resArb := Arbitrage{
			Path: []ArbitragePathUnit{},
		}
		for i, edge := range arb.UsedEdges {
			pathUnitRes := ArbitragePathUnit{
				PoolAddress:     edge.Exchangable.Address(),
				TokenInAddress:  edge.Exchangable.GetToken0().Address,
				TokenOutAddress: edge.Exchangable.GetToken1().Address,
				AmountIn:        arb.Amounts[i],
				AmountOut:       arb.Amounts[i+1],
			}

			if !edge.Zfo {
				pathUnitRes = ArbitragePathUnit{
					PoolAddress:     edge.Exchangable.Address(),
					TokenInAddress:  edge.Exchangable.GetToken1().Address,
					TokenOutAddress: edge.Exchangable.GetToken0().Address,
					AmountIn:        arb.Amounts[i],
					AmountOut:       arb.Amounts[i+1],
				}
			}

			resArb.Path = append(resArb.Path, pathUnitRes)
		}

		res = append(res, resArb)
	}

	return res, nil
}

func (s *arbitrageService) FindOldArbs() error {
	swaps, err := s.v3TransactionsDBRepo.GetV3SwapsByChainID(s.chainID)
	if err != nil {
		return err
	}
	fmt.Println("Swaps: ", len(swaps))

	transactionsMap := map[string][]models.V3Swap{}
	for _, swap := range swaps {
		if arr, ok := transactionsMap[swap.TxHash]; ok {
			arr = append(arr, swap)
			transactionsMap[swap.TxHash] = arr
		} else {
			transactionsMap[swap.TxHash] = []models.V3Swap{swap}
		}
	}

	pools, err := s.v3PoolDBRepo.GetPoolsByChainID(s.chainID)
	if err != nil {
		return err
	}
	fmt.Println("Pools: ", len(pools))

	poolsMap := map[string]models.UniswapV3Pool{}
	for _, pool := range pools {
		poolsMap[pool.Address] = pool
	}

	arbs := []oldArb{}

transactionLoop:
	for txHash, swaps := range transactionsMap {
		swapsMap := map[int]*models.V3Swap{}
		amountsOut := map[int]*big.Int{}

		for _, swap := range swaps {
			swapsMap[swap.ID] = &swap
			amountOut := swap.Amount1
			if swap.Amount0.Sign() < 0 {
				amountOut = swap.Amount0
			}

			amountsOut[swap.ID] = amountOut
		}

		nodesMap := map[int]*SwapChainNode{}

		var randomSwapNode *SwapChainNode
		for _, swap := range swaps {
			amountIn := swap.Amount1
			if swap.Amount0.Sign() > 0 {
				amountIn = swap.Amount0
			}

			for outID, amountOut := range amountsOut {
				if amountIn.CmpAbs(amountOut) == 0 {
					swapNode, ok := nodesMap[outID]
					if !ok {
						swapNode = &SwapChainNode{
							SwapID:       outID,
							NextSwapNode: nil,
							PrevSwapNode: nil,
						}
					}

					nextSwapNode, ok := nodesMap[swap.ID]
					if !ok {
						nextSwapNode = &SwapChainNode{
							SwapID:       swap.ID,
							NextSwapNode: nil,
						}
					}
					nextSwapNode.PrevSwapNode = swapNode
					swapNode.NextSwapNode = nextSwapNode
					nodesMap[outID] = swapNode
					nodesMap[swap.ID] = nextSwapNode

					randomSwapNode = swapNode
					break
				}
			}
		}

		if randomSwapNode == nil || (randomSwapNode.PrevSwapNode == nil && randomSwapNode.NextSwapNode == nil) {
			continue
		}

		lastSwapNode := randomSwapNode
		for lastSwapNode.PrevSwapNode != nil {
			lastSwapNode = lastSwapNode.PrevSwapNode
		}

		count := 1
		firstSwapNode := lastSwapNode
		for firstSwapNode.NextSwapNode != nil {
			firstSwapNode = firstSwapNode.NextSwapNode
			count++
		}

		if count >= 3 {
			arb := oldArb{
				txHash:     txHash,
				pathString: "",
				tokens:     []string{},
				amountIn:   new(big.Int),
				amountOut:  new(big.Int),
			}

			firstSwapNode := lastSwapNode
			swap, ok := swapsMap[firstSwapNode.SwapID]
			if !ok {
				continue
			}

			arb.pathString += fmt.Sprint(swap.PoolAddress, "->")
			pool, ok := poolsMap[swap.PoolAddress]
			if !ok {
				continue transactionLoop
			}

			zfo := swap.Amount0.Cmp(big.NewInt(0)) > 0

			tokenIn := ""
			tokenOut := ""
			if zfo {
				tokenIn = pool.Token0
				tokenOut = pool.Token1
				arb.amountIn.Abs(swap.Amount0)
				arb.amountOut.Abs(swap.Amount1)
			} else {
				tokenIn = pool.Token1
				tokenOut = pool.Token0
				arb.amountIn.Abs(swap.Amount1)
				arb.amountOut.Abs(swap.Amount0)
			}

			arb.tokens = append(arb.tokens, tokenIn)
			arb.tokens = append(arb.tokens, tokenOut)

			for firstSwapNode.NextSwapNode != nil {
				firstSwapNode = firstSwapNode.NextSwapNode

				swap, ok := swapsMap[firstSwapNode.SwapID]
				arb.t = int(swap.TxTimestamp)
				if !ok {
					continue
				}
				arb.pathString += fmt.Sprint(swap.PoolAddress, "->")

				pool, ok := poolsMap[swap.PoolAddress]
				if !ok {
					continue transactionLoop
				}

				zfo := swap.Amount0.Cmp(big.NewInt(0)) > 0

				tokenOut := ""
				if zfo {
					tokenOut = pool.Token1
					arb.amountIn.Abs(swap.Amount0)
					arb.amountOut.Abs(swap.Amount1)
				} else {
					tokenOut = pool.Token0
					arb.amountIn.Abs(swap.Amount1)
					arb.amountOut.Abs(swap.Amount0)
				}

				arb.tokens = append(arb.tokens, tokenOut)

			}
			if arb.tokens[0] == arb.tokens[len(arb.tokens)-1] {
				arbs = append(arbs, arb)
			}

		}

	}

	slices.SortFunc(arbs, func(x oldArb, y oldArb) int {
		return x.t - y.t
	})
	for _, arb := range arbs {
		fmt.Println(time.Unix(int64(arb.t), 0))
		fmt.Println(arb.pathString)
		fmt.Println(arb.txHash)
		fmt.Println("=============")

	}

	return nil
}
