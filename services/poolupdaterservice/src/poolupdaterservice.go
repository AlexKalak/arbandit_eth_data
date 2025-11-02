package poolupdaterservice

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/alexkalak/go_market_analyze/common/models"
	"github.com/alexkalak/go_market_analyze/common/repo/exchangerepo/v3poolsrepo"
	"github.com/alexkalak/go_market_analyze/common/repo/tokenrepo"
	"github.com/alexkalak/go_market_analyze/common/repo/transactionrepo/v3transactionrepo"
)

type PoolUpdaterService interface {
	Start(ctx context.Context) error
}

type PoolUpdaterServiceDependencies struct {
	V3PoolDBRepo        v3poolsrepo.V3PoolDBRepo
	V3TransactionDBRepo v3transactionrepo.V3TransactionDBRepo
	V3PoolCacheRepo     v3poolsrepo.V3PoolCacheRepo
	TokenDBRepo         tokenrepo.TokenRepo
}

func (d *PoolUpdaterServiceDependencies) validate() error {
	if d.V3PoolDBRepo == nil {
		return errors.New("pool updater service dependencies V3PoolDBRepo cannot be nil")
	}

	if d.V3PoolCacheRepo == nil {

		return errors.New("pool updater service dependencies V3PoolCacheRepo cannot be nil")
	}

	if d.TokenDBRepo == nil {
		return errors.New("pool updater service dependencies TokenRepo cannot be nil")
	}

	if d.V3TransactionDBRepo == nil {
		return errors.New("pool updater service dependencies V3TransactionDBRepo cannot be nil")
	}

	return nil
}

type PoolUpdaterServiceConfig struct {
	ChainID                 uint
	KafkaServer             string
	KafkaUpdateV3PoolsTopic string
}

func (d *PoolUpdaterServiceConfig) validate() error {
	if d.ChainID == 0 {
		return errors.New("pool updater service config ChainID not set")
	}

	if d.KafkaServer == "" {
		return errors.New("pool updater service config KafkaServer not set")
	}

	if d.KafkaUpdateV3PoolsTopic == "" {
		return errors.New("pool updater service config KafkaUpdateV3PoolsTopic not set")
	}

	return nil
}

type poolUpdaterService struct {
	tokensMap map[models.TokenIdentificator]*models.Token

	currentCheckingBlock    uint64
	currentBlockPoolChanges map[models.V3PoolIdentificator]models.UniswapV3Pool

	config PoolUpdaterServiceConfig

	tokenDBRepo         tokenrepo.TokenRepo
	v3PoolDBRepo        v3poolsrepo.V3PoolDBRepo
	v3PoolCacheRepo     v3poolsrepo.V3PoolCacheRepo
	v3TransactionDBRepo v3transactionrepo.V3TransactionDBRepo
}

func New(config PoolUpdaterServiceConfig, dependencies PoolUpdaterServiceDependencies) (PoolUpdaterService, error) {
	if err := dependencies.validate(); err != nil {
		return nil, err
	}
	if err := config.validate(); err != nil {
		return nil, err
	}

	service := poolUpdaterService{
		tokenDBRepo:             dependencies.TokenDBRepo,
		config:                  config,
		v3PoolDBRepo:            dependencies.V3PoolDBRepo,
		v3PoolCacheRepo:         dependencies.V3PoolCacheRepo,
		v3TransactionDBRepo:     dependencies.V3TransactionDBRepo,
		currentCheckingBlock:    0,
		currentBlockPoolChanges: map[models.V3PoolIdentificator]models.UniswapV3Pool{},
	}

	tokens, err := service.tokenDBRepo.GetTokensByChainID(config.ChainID)
	if err != nil {
		return nil, err
	}

	service.tokensMap = map[models.TokenIdentificator]*models.Token{}
	for _, token := range tokens {
		service.tokensMap[token.GetIdentificator()] = &token
	}

	pools, err := service.v3PoolDBRepo.GetPoolsByChainID(config.ChainID)
	if err != nil {
		return nil, err
	}

	service.ConfigureCache(pools)

	return &service, nil
}

func (s *poolUpdaterService) ConfigureCache(pools []models.UniswapV3Pool) error {
	var maxBlockNumber int64 = math.MinInt64
	for _, pool := range pools {
		if int64(pool.BlockNumber) > maxBlockNumber {
			maxBlockNumber = int64(pool.BlockNumber)
		}
	}

	if maxBlockNumber == math.MinInt64 {
		return errors.New("unable to define block_number for pools")
	}

	err := s.v3PoolCacheRepo.ClearPools(s.config.ChainID)
	if err != nil {
		return err
	}

	fmt.Println("Setting block number to: ", maxBlockNumber)
	err = s.v3PoolCacheRepo.SetBlockNumber(s.config.ChainID, uint64(maxBlockNumber))
	if err != nil {
		return err
	}

	if len(pools) > 0 {
		err = s.v3PoolCacheRepo.SetPools(s.config.ChainID, pools)
		if err != nil {
			return err
		}

	}

	return nil
}
