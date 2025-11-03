package v2

import "errors"

// Predefined errors to avoid dynamic errors.
var (
	ErrEmptyBrokersList    = errors.New("brokers list cannot be empty")
	ErrEmptyBrokersString  = errors.New("brokers string cannot be empty")
	ErrNoBrokersDiscovered = errors.New("no brokers discovered")
	ErrProduceFailed       = errors.New("produce failed")
)
