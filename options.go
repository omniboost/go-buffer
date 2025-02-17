package buffer

import (
	"errors"
	"fmt"
	"time"
)

const (
	invalidSize     = "size cannot be zero"
	invalidFlusher  = "flusher cannot be nil"
	invalidInterval = "interval must be greater than zero (%s)"
	invalidTimeout  = "timeout cannot be negative (%s)"
)

type (
	// Option setter.
	Option[T any] func(*Buffer[T])
)

// WithSize sets the size of the buffer.
func WithSize(size uint) Option[any] {
	return func(options *Buffer[any]) {
		options.Size = size
	}
}

// WithFlusher sets the flusher that should be used to write out the buffer.
func WithFlusher[T any](flusher Flusher[T]) Option[T] {
	return func(options *Buffer[T]) {
		options.Flusher = flusher
	}
}

// WithFlushInterval sets the interval between automatic flushes.
func WithFlushInterval(interval time.Duration) Option[any] {
	return func(options *Buffer[any]) {
		options.FlushInterval = interval
	}
}

// WithPushTimeout sets how long a push should wait before giving up.
func WithPushTimeout(timeout time.Duration) Option[any] {
	return func(options *Buffer[any]) {
		options.PushTimeout = timeout
	}
}

// WithFlushTimeout sets how long a manual flush should wait before giving up.
func WithFlushTimeout(timeout time.Duration) Option[any] {
	return func(options *Buffer[any]) {
		options.FlushTimeout = timeout
	}
}

// WithCloseTimeout sets how long
func WithCloseTimeout(timeout time.Duration) Option[any] {
	return func(options *Buffer[any]) {
		options.CloseTimeout = timeout
	}
}

func validateBuffer[T any](options *Buffer[T]) error {
	if options.Size == 0 {
		return errors.New(invalidSize)
	}
	if options.Flusher == nil {
		return errors.New(invalidFlusher)
	}
	if options.FlushInterval < 0 {
		return fmt.Errorf(invalidInterval, "FlushInterval")
	}
	if options.PushTimeout < 0 {
		return fmt.Errorf(invalidTimeout, "PushTimeout")
	}
	if options.FlushTimeout < 0 {
		return fmt.Errorf(invalidTimeout, "FlushTimeout")
	}
	if options.CloseTimeout < 0 {
		return fmt.Errorf(invalidTimeout, "CloseTimeout")
	}

	return nil
}
