package store

import "github.com/jackc/pgx/v5/pgxpool"

type RunsRepo struct {
	Pool *pgxpool.Pool
}

func NewRunsRepo(pool *pgxpool.Pool) *RunsRepo {
	return &RunsRepo{Pool: pool}
}
