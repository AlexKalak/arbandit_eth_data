package exchangables

import (
	"math/big"

	"github.com/alexkalak/go_market_analyze/common/models"
)

type Exchangable interface {
	ImitateSwap(amountIn *big.Int, zfo bool) (*big.Int, error)
	GetRate(zfo bool) *big.Float
	Address() string
	GetToken0() *models.Token
	GetToken1() *models.Token
	GetIdentifier() string
}
