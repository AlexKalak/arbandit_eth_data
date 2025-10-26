package merger

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/alexkalak/go_market_analyze/common/models"
)

func (m *merger) MergeTokens(chainID uint) error {
	db, err := m.database.GetDB()
	if err != nil {
		return err
	}

	tokens, err := m.subgraphClient.GetTokensForV3Contract(context.Background(), chainID)
	if err != nil {
		return err
	}

	query := psql.Insert(models.TOKENS_TABLE).Columns(
		models.TOKEN_ADDRESS,
		models.TOKEN_CHAINID,
		models.TOKEN_NAME,
		models.TOKEN_SYMBOL,
		models.TOKEN_LOGOURI,
		models.TOKEN_DECIMALS,
		models.TOKEN_DEFI_USD_PRICE,
	)

	tokensMap := map[string]any{}

	fmt.Println(len(tokens))
	for i, token := range tokens {
		if _, ok := tokensMap[token.Address]; ok {
			fmt.Println("found duplicate")
			continue
		}
		tokensMap[token.Address] = new(any)

		query = query.Values(
			token.Address,
			token.ChainID,
			token.Name,
			token.Symbol,
			token.LogoURI,
			token.Decimals,
			//defi price shoudlnt be null
			0,
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
			fmt.Println("Tokens inserted", rowsAff)

			query = psql.Insert(models.TOKENS_TABLE).Columns(
				models.TOKEN_ADDRESS,
				models.TOKEN_CHAINID,
				models.TOKEN_NAME,
				models.TOKEN_SYMBOL,
				models.TOKEN_LOGOURI,
				models.TOKEN_DECIMALS,
				models.TOKEN_DEFI_USD_PRICE,
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
	fmt.Println("Tokens inserted", rowsAff)

	return nil
}
