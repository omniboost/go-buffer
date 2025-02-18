package buffer_test

import (
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/omniboost/go-buffer"
)

var _ = Describe("Options", func() {
	It("sets up size", func() {
		// arrange
		opts := buffer.New[any]()

		// act
		opts = opts.WithSize(10)

		// assert
		Expect(opts.Size).To(BeIdenticalTo(uint(10)))
	})

	It("sets up flusher", func() {
		// arrange
		opts := buffer.New[any]()
		flusher := func(items []interface{}) {}

		// act
		opts = opts.WithFlusher(buffer.FlusherFunc[any](flusher))

		// assert
		Expect(opts.Flusher).NotTo(BeNil())
	})

	It("sets up flush interval", func() {
		// arrange
		opts := buffer.New[any]()

		// act
		opts = opts.WithFlushInterval(5 * time.Second)

		// assert
		Expect(opts.FlushInterval).To(Equal(5 * time.Second))
	})

	It("sets up push timeout", func() {
		// arrange
		opts := buffer.New[any]()

		// act
		opts = opts.WithPushTimeout(10 * time.Second)

		// assert
		Expect(opts.PushTimeout).To(Equal(10 * time.Second))
	})

	It("sets up flush timeout", func() {
		// arrange
		opts := buffer.New[any]()

		// act
		opts = opts.WithFlushTimeout(15 * time.Second)

		// assert
		Expect(opts.FlushTimeout).To(Equal(15 * time.Second))
	})

	It("sets up close timeout", func() {
		// arrange
		opts := buffer.New[any]()

		// act
		opts = opts.WithCloseTimeout(3 * time.Second)

		// assert
		Expect(opts.CloseTimeout).To(Equal(3 * time.Second))
	})
})
