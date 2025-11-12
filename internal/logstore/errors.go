package logstore

import "errors"

var (
	// ErrTransactionsDisabled is returned when attempting to use transactional
	// APIs without enabling the delta log integration.
	ErrTransactionsDisabled = errors.New("log transactions are disabled")
	// ErrEmptyTransaction indicates that no records were appended before commit.
	ErrEmptyTransaction = errors.New("transaction contains no records")
	// ErrTransactionClosed reports misuse after commit/abort.
	ErrTransactionClosed = errors.New("transaction already closed")
	// ErrTxnConflict surfaces optimistic concurrency failures from the delta log.
	ErrTxnConflict = errors.New("transaction conflict")
)
