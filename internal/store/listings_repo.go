package store

import "github.com/jackc/pgx/v5/pgxpool"

type ListingsRepo struct {
	Pool *pgxpool.Pool
}

func NewListingsRepo(pool *pgxpool.Pool) *ListingsRepo {
	return &ListingsRepo{Pool: pool}
}
