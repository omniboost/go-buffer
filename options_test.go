package buffer_test

import (
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/globocom/go-buffer/v2"
)

var _ = Describe("Options", func() {
	It("sets up size", func() {
		// arrange
		opts := &buffer.Buffer[any]{}

		// act
		buffer.WithSize(10)(opts)

		// assert
		Expect(opts.Size).To(BeIdenticalTo(uint(10)))
	})

	It("sets up flusher", func() {
		// arrange
		opts := &buffer.Buffer[any]{}
		flusher := func(items []interface{}) {}

		// act
		buffer.WithFlusher(buffer.FlusherFunc(flusher))(opts)

		// assert
		Expect(opts.Flusher).NotTo(BeNil())
	})

	It("sets up flush interval", func() {
		// arrange
		opts := &buffer.Buffer[any]{}

		// act
		buffer.WithFlushInterval(5 * time.Second)(opts)

		// assert
		Expect(opts.FlushInterval).To(Equal(5 * time.Second))
	})

	It("sets up push timeout", func() {
		// arrange
		opts := &buffer.Buffer[any]{}

		// act
		buffer.WithPushTimeout(10 * time.Second)(opts)

		// assert
		Expect(opts.PushTimeout).To(Equal(10 * time.Second))
	})

	It("sets up flush timeout", func() {
		// arrange
		opts := &buffer.Buffer[any]{}

		// act
		buffer.WithFlushTimeout(15 * time.Second)(opts)

		// assert
		Expect(opts.FlushTimeout).To(Equal(15 * time.Second))
	})

	It("sets up close timeout", func() {
		// arrange
		opts := &buffer.Buffer[any]{}

		// act
		buffer.WithCloseTimeout(3 * time.Second)(opts)

		// assert
		Expect(opts.CloseTimeout).To(Equal(3 * time.Second))
	})
})
