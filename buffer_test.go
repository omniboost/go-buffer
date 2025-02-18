package buffer_test

import (
	"fmt"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/omniboost/go-buffer"
)

var _ = Describe("Buffer", func() {
	var flusher *MockFlusher[any]

	BeforeEach(func() {
		flusher = NewMockFlusher[any]()
	})

	Context("Constructor", func() {
		It("creates a new Buffer instance", func() {
			// act
			sut := buffer.New[any]().
				WithSize(10).
				WithFlusher(flusher)

			// assert
			Expect(sut).NotTo(BeNil())
		})

		Context("generics", func() {
			It("allows for generic types", func() {
				buf := buffer.New[int]().
					WithSize(10).
					WithFlusher(NewMockFlusher[int]())

				err := buf.Push(0)

				// we expect no error
				Expect(err).To(BeNil())
			})
		})

		Context("invalid options", func() {
			It("panics when provided an invalid size", func() {
				buf := buffer.New[any]().
					WithSize(0)

				err := buf.Push(0)

				Expect(err).To(MatchError(buffer.ErrInvalidSize))
			})

			It("panics when provided an invalid flusher", func() {
				buf := buffer.New[any]().
					WithSize(1).
					WithFlusher(nil)

				err := buf.Push(0)

				Expect(err).To(MatchError(buffer.ErrInvalidFlusher))
			})

			It("panics when provided an invalid flush interval", func() {
				buf := buffer.New[any]().
					WithSize(1).
					WithFlusher(flusher).
					WithFlushInterval(-1)

				err := buf.Push(0)

				Expect(err).To(MatchError(fmt.Errorf(buffer.ErrInvalidInterval, "FlushInterval")))
			})

			It("panics when provided an invalid push timeout", func() {
				buf := buffer.New[any]().
					WithSize(1).
					WithFlusher(flusher).
					WithPushTimeout(-1)

				err := buf.Push(0)

				Expect(err).To(MatchError(fmt.Errorf(buffer.ErrInvalidTimeout, "PushTimeout")))
			})

			It("panics when provided an invalid flush timeout", func() {
				buf := buffer.New[any]().
					WithSize(1).
					WithFlusher(flusher).
					WithFlushTimeout(-1)

				err := buf.Push(0)

				Expect(err).To(MatchError(fmt.Errorf(buffer.ErrInvalidTimeout, "FlushTimeout")))
			})

			It("panics when provided an invalid close timeout", func() {
				buf := buffer.New[any]().
					WithSize(1).
					WithFlusher(flusher).
					WithCloseTimeout(-1)

				err := buf.Push(0)

				Expect(err).To(MatchError(fmt.Errorf(buffer.ErrInvalidTimeout, "CloseTimeout")))
			})
		})
	})

	Context("Pushing", func() {
		It("pushes items into the buffer when Push is called", func() {
			// arrange
			sut := buffer.New[any]().
				WithSize(3).
				WithFlusher(flusher)

			// act
			err1 := sut.Push(1)
			err2 := sut.Push(2)
			err3 := sut.Push(3)

			// assert
			Expect(err1).To(Succeed())
			Expect(err2).To(Succeed())
			Expect(err3).To(Succeed())
		})

		It("fails when Push cannot execute in a timely fashion", func() {
			// arrange
			flusher.Func = func() { select {} }
			sut := buffer.New[any]().
				WithSize(2).
				WithFlusher(flusher).
				WithPushTimeout(time.Second)

			// act
			err1 := sut.Push(1)
			err2 := sut.Push(2)
			err3 := sut.Push(3)

			// assert
			Expect(err1).To(Succeed())
			Expect(err2).To(Succeed())
			Expect(err3).To(MatchError(buffer.ErrTimeout))
		})

		It("fails when the buffer is closed", func() {
			// arrange
			sut := buffer.New[any]().
				WithSize(2).
				WithFlusher(flusher)

			err := sut.Push(0)

			_ = sut.Close()

			// act
			err1 := sut.Push(1)

			// assert
			Expect(err).To(Succeed())
			Expect(err1).To(MatchError(buffer.ErrClosed))
		})
	})

	Context("Flushing", func() {
		It("flushes the buffer when it fills up", func(done Done) {
			// arrange
			sut := buffer.New[any]().
				WithSize(5).
				WithFlusher(flusher)

			// act
			err := sut.Push(1)
			_ = sut.Push(2)
			_ = sut.Push(3)
			_ = sut.Push(4)
			_ = sut.Push(5)

			Expect(err).To(Succeed())

			// assert
			result := <-flusher.Done
			Expect(result.Items).To(ConsistOf(1, 2, 3, 4, 5))
			close(done)
		})

		It("flushes the buffer when the provided interval has elapsed", func(done Done) {
			// arrange
			interval := 3 * time.Second
			start := time.Now()
			sut := buffer.New[any]().
				WithSize(5).
				WithFlusher(flusher).
				WithFlushInterval(interval)

			// act
			err := sut.Push(1)

			// assert
			result := <-flusher.Done
			Expect(err).To(Succeed())
			Expect(result.Items).To(ConsistOf(1))
			Expect(result.Time).To(BeTemporally("~", start, interval+time.Second))
			close(done)
		}, 5)

		It("flushes the buffer when Flush is called", func(done Done) {
			// arrange
			sut := buffer.New[any]().
				WithSize(3).
				WithFlusher(flusher)

			err := sut.Push(1)
			_ = sut.Push(2)

			// act
			err1 := sut.Flush()

			// assert
			result := <-flusher.Done
			Expect(err).To(Succeed())
			Expect(err1).To(Succeed())
			Expect(result.Items).To(ConsistOf(1, 2))
			close(done)
		})

		It("fails when Flush cannot execute in a timely fashion", func() {
			// arrange
			flusher.Func = func() { time.Sleep(3 * time.Second) }
			sut := buffer.New[any]().
				WithSize(1).
				WithFlusher(flusher).
				WithFlushTimeout(time.Second)

			err := sut.Push(1)

			// act
			err1 := sut.Flush()

			// assert
			Expect(err).To(Succeed())
			Expect(err1).To(MatchError(buffer.ErrTimeout))
		})

		It("fails when the buffer is closed", func() {
			// arrange
			sut := buffer.New[any]().
				WithSize(2).
				WithFlusher(flusher)

			err := sut.Push(1)

			_ = sut.Close()

			// act
			err1 := sut.Flush()

			// assert
			Expect(err).To(Succeed())
			Expect(err1).To(MatchError(buffer.ErrClosed))
		})
	})

	Context("Closing", func() {
		It("flushes the buffer and closes it when Close is called", func(done Done) {
			// arrange
			sut := buffer.New[any]().
				WithSize(3).
				WithFlusher(flusher)

			err := sut.Push(1)
			_ = sut.Push(2)

			// act
			err1 := sut.Close()

			// assert
			result := <-flusher.Done
			Expect(err).To(Succeed())
			Expect(err1).To(Succeed())
			Expect(result.Items).To(ConsistOf(1, 2))
			close(done)
		})

		It("fails when Close cannot execute in a timely fashion", func() {
			// arrange
			flusher.Func = func() { time.Sleep(2 * time.Second) }

			sut := buffer.New[any]().
				WithSize(1).
				WithFlusher(flusher).
				WithCloseTimeout(time.Second)

			err := sut.Push(1)

			// act
			err1 := sut.Close()

			// assert
			Expect(err).To(Succeed())
			Expect(err1).To(MatchError(buffer.ErrTimeout))
		})

		It("fails when the buffer is closed", func() {
			// arrange
			flusher.Func = func() {}

			sut := buffer.New[any]().
				WithSize(1).
				WithFlusher(flusher).
				WithCloseTimeout(time.Second)

			err := sut.Push(0)

			// act
			_ = sut.Close()
			err1 := sut.Close()

			// assert
			Expect(err).To(Succeed())
			Expect(err1).To(MatchError(buffer.ErrClosed))
		})

		It("allows Close to be called again if it fails", func() {
			// arrange
			flusher.Func = func() { time.Sleep(2 * time.Second) }

			sut := buffer.New[any]().
				WithSize(1).
				WithFlusher(flusher).
				WithCloseTimeout(time.Second)

			err := sut.Push(1)

			// act
			err1 := sut.Close()
			time.Sleep(time.Second)
			err2 := sut.Close()

			// assert
			Expect(err).To(Succeed())
			Expect(err1).To(MatchError(buffer.ErrTimeout))
			Expect(err2).To(Succeed())
		})
	})
})

type (
	MockFlusher[T any] struct {
		Done chan *WriteCall[T]
		Func func()
	}

	WriteCall[T any] struct {
		Time  time.Time
		Items []T
	}
)

func (flusher *MockFlusher[T]) Write(items []T) {
	call := &WriteCall[T]{
		Time:  time.Now(),
		Items: items,
	}

	if flusher.Func != nil {
		flusher.Func()
	}

	flusher.Done <- call
}

func NewMockFlusher[T any]() *MockFlusher[T] {
	return &MockFlusher[T]{
		Done: make(chan *WriteCall[T], 1),
	}
}
