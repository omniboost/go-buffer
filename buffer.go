package buffer

import (
	"errors"
	"io"
	"time"
)

var (
	// ErrTimeout indicates an operation has timed out.
	ErrTimeout = errors.New("operation timed-out")
	// ErrClosed indicates the buffer is closed and can no longer be used.
	ErrClosed = errors.New("buffer is closed")
)

type (
	// Buffer represents a data buffer that is asynchronously flushed, either manually or automatically.
	Buffer[T any] struct {
		io.Closer
		dataCh  chan T
		flushCh chan struct{}
		closeCh chan struct{}
		doneCh  chan struct{}

		// options
		Size          uint
		Flusher       Flusher[T]
		FlushInterval time.Duration
		PushTimeout   time.Duration
		FlushTimeout  time.Duration
		CloseTimeout  time.Duration
	}
)

// Push appends an item to the end of the buffer.
//
// It returns an ErrTimeout if if cannot be performed in a timely fashion, and
// an ErrClosed if the buffer has been closed.
func (buffer *Buffer[T]) Push(item T) error {
	if !buffer.IsIntialized() {
		// validate the options
		err := buffer.Validate()
		if err != nil {
			return err
		}

		// initialize the buffer
		err = buffer.initialize()
		if err != nil {
			return err
		}
	}

	if buffer.closed() {
		return ErrClosed
	}

	select {
	case buffer.dataCh <- item:
		return nil
	case <-time.After(buffer.PushTimeout):
		return errors.Join(errors.New("buffer is full"), ErrTimeout)
	}
}

// Flush outputs the buffer to a permanent destination.
//
// It returns an ErrTimeout if if cannot be performed in a timely fashion, and
// an ErrClosed if the buffer has been closed.
func (buffer *Buffer[T]) Flush() error {
	if buffer.closed() {
		return ErrClosed
	}

	select {
	case buffer.flushCh <- struct{}{}:
		return nil
	case <-time.After(buffer.FlushTimeout):
		return errors.Join(errors.New("failed to flush buffer within flush timeout"), ErrTimeout)
	}
}

// Close flushes the buffer and prevents it from being further used.
//
// It returns an ErrTimeout if if cannot be performed in a timely fashion, and
// an ErrClosed if the buffer has already been closed.
//
// An ErrTimeout can either mean that a flush could not be triggered, or it can
// mean that a flush was triggered but it has not finished yet. In any case it is
// safe to call Close again.
func (buffer *Buffer[T]) Close() error {
	if buffer.closed() {
		return ErrClosed
	}

	select {
	case buffer.closeCh <- struct{}{}:
		// noop
	case <-time.After(buffer.CloseTimeout):
		return errors.Join(errors.New("failed to close buffer within close timeout"), ErrTimeout)
	}

	select {
	case <-buffer.doneCh:
		close(buffer.dataCh)
		close(buffer.flushCh)
		close(buffer.closeCh)
		return nil
	case <-time.After(buffer.CloseTimeout):
		return errors.Join(errors.New("failed to close buffer within close timeout"), ErrTimeout)
	}
}

func (buffer Buffer[T]) closed() bool {
	select {
	case <-buffer.doneCh:
		return true
	default:
		return false
	}
}

func (buffer *Buffer[T]) consume() {
	count := 0
	items := make([]T, buffer.Size)
	mustFlush := false
	ticker, stopTicker := newTicker(buffer.FlushInterval)

	isOpen := true
	for isOpen {
		select {
		case item := <-buffer.dataCh:
			items[count] = item
			count++
			mustFlush = count >= len(items)
		case <-ticker:
			mustFlush = count > 0
		case <-buffer.flushCh:
			mustFlush = count > 0
		case <-buffer.closeCh:
			isOpen = false
			mustFlush = count > 0
		}

		if mustFlush {
			stopTicker()
			buffer.Flusher.Write(items[:count])

			count = 0
			items = make([]T, buffer.Size)
			mustFlush = false
			ticker, stopTicker = newTicker(buffer.FlushInterval)
		}
	}

	stopTicker()
	close(buffer.doneCh)
}

func newTicker(interval time.Duration) (<-chan time.Time, func()) {
	if interval == 0 {
		return nil, func() {}
	}

	ticker := time.NewTicker(interval)
	return ticker.C, ticker.Stop
}

// New creates a new buffer instance with the provided options.
func New[T any](opts ...Option[T]) *Buffer[T] {
	buffer := &Buffer[T]{
		// Options
		Size:          0,
		Flusher:       nil,
		FlushInterval: 0,
		PushTimeout:   time.Second,
		FlushTimeout:  time.Second,
		CloseTimeout:  time.Second,
	}

	for _, opt := range opts {
		opt(buffer)
	}

	return buffer
}

func (b *Buffer[T]) Validate() error {
	return validateBuffer(b)
}

func (b *Buffer[T]) IsIntialized() bool {
	return b.dataCh != nil
}

func (b *Buffer[T]) initialize() error {
	err := validateBuffer(b)
	if err != nil {
		return err
	}

	b.dataCh = make(chan T)
	b.flushCh = make(chan struct{})
	b.closeCh = make(chan struct{})
	b.doneCh = make(chan struct{})

	go b.consume()

	return nil
}
