package pgdatabase

import (
	"database/sql"
	"errors"
	"fmt"

	_ "github.com/lib/pq"
)

type PgDatabaseConfig struct {
	Host     string
	Port     string
	User     string
	Password string
	DBName   string
	SSlMode  string
}

type PgDatabase struct {
	db *sql.DB
}

func (d *PgDatabase) GetDB() (*sql.DB, error) {
	if d.db == nil {
		return nil, errors.New("pg database uninitialized")
	}

	return d.db, nil
}

func New(config PgDatabaseConfig) (*PgDatabase, error) {

	connStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s ",
		config.Host, config.Port, config.User, config.Password, config.DBName, config.SSlMode)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, err
	}

	return &PgDatabase{
		db: db,
	}, nil
}
