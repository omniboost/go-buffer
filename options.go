package buffer

import (
	"errors"
	"fmt"
	"time"
)

const (
	ErrInvalidSize     = "size cannot be zero"
	ErrInvalidFlusher  = "flusher cannot be nil"
	ErrInvalidInterval = "interval must be greater than zero (%s)"
	ErrInvalidTimeout  = "timeout cannot be negative (%s)"
)

type (
	// Option setter.
	Option[T any] func(*Buffer[T])
)

// WithSize sets the size of the buffer.
func (b *Buffer[T]) WithSize(size uint) *Buffer[T] {
	b.Size = size
	return b
}

// WithFlusher sets the flusher that should be used to write out the buffer.
func (b *Buffer[T]) WithFlusher(flusher Flusher[T]) *Buffer[T] {
	b.Flusher = flusher
	return b
}

// WithFlushInterval sets the interval between automatic flushes.
func (b *Buffer[T]) WithFlushInterval(interval time.Duration) *Buffer[T] {
	b.FlushInterval = interval
	return b
}

// WithPushTimeout sets how long a push should wait before giving up.
func (b *Buffer[T]) WithPushTimeout(timeout time.Duration) *Buffer[T] {
	b.PushTimeout = timeout
	return b
}

// WithFlushTimeout sets how long a manual flush should wait before giving up.
func (b *Buffer[T]) WithFlushTimeout(timeout time.Duration) *Buffer[T] {
	b.FlushTimeout = timeout
	return b
}

// WithCloseTimeout sets how long
func (b *Buffer[T]) WithCloseTimeout(timeout time.Duration) *Buffer[T] {
	b.CloseTimeout = timeout
	return b
}

func validateBuffer[T any](options *Buffer[T]) error {
	if options.Size == 0 {
		return errors.New(ErrInvalidSize)
	}
	if options.Flusher == nil {
		return errors.New(ErrInvalidFlusher)
	}
	if options.FlushInterval < 0 {
		return fmt.Errorf(ErrInvalidInterval, "FlushInterval")
	}
	if options.PushTimeout < 0 {
		return fmt.Errorf(ErrInvalidTimeout, "PushTimeout")
	}
	if options.FlushTimeout < 0 {
		return fmt.Errorf(ErrInvalidTimeout, "FlushTimeout")
	}
	if options.CloseTimeout < 0 {
		return fmt.Errorf(ErrInvalidTimeout, "CloseTimeout")
	}

	return nil
}
