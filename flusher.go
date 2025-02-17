package buffer

type (
	// Flusher represents a destination of buffered data.
	Flusher[T any] interface {
		Write(items []T)
	}

	// FlusherFunc represents a flush function.
	FlusherFunc func(items []interface{})
)

func (fn FlusherFunc) Write(items []interface{}) {
	fn(items)
}
