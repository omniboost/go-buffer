<p align="center">
  <img src="gopher.png">
</p>
<p align="center">
  <img src="https://img.shields.io/github/workflow/status/omniboost/go-buffer/Go?style=flat-square">
  <a href="https://github.com/omniboost/go-buffer/blob/master/LICENSE">
    <img src="https://img.shields.io/github/license/omniboost/go-buffer?color=blue&style=flat-square">
  </a>
  <img src="https://img.shields.io/github/go-mod/go-version/omniboost/go-buffer?style=flat-square">
  <a href="https://pkg.go.dev/github.com/omniboost/go-buffer">
    <img src="https://img.shields.io/badge/Go-reference-blue?style=flat-square">
  </a>
</p>

# go-buffer

`go-buffer` represents a buffer that asynchronously flushes its contents. It is useful for applications that need to aggregate data before writing it to an external storage. A buffer is flushed manually, or automatically when it becomes full or after an interval has elapsed, whichever comes first.

## Installation

    go get github.com/omniboost/go-buffer

## Examples

### Size-triggered flush

```golang
package main

import (
  "time"

  "github.com/omniboost/go-buffer"
)

func main() {
  buff, err := buffer.New[any]().
      // buffer can hold up to 5 items
      buffer.WithSize(5).
      // call this function when the buffer needs flushing
      buffer.WithFlusher(buffer.FlusherFunc(func(items []interface{}) {
        for _, item := range items {
          println(item.(string))
        }
      })).
      // Consume
      Consume()

  if err != nil {
    panic(err)
  }

  // ensure the buffer
  defer buff.Close()

  buff.Push("item 1")
  buff.Push("item 2")
  buff.Push("item 3")
  buff.Push("item 4")
  buff.Push("item 5")

  // block the current goroutine
  time.Sleep(3 * time.Second)

  println("done")
}
```

### Interval-triggered flush

```golang
package main

import (
  "time"

  "github.com/omniboost/go-buffer"
)

func main() {
  buff, err := buffer.New[any]().
    // buffer can hold up to 5 items
    buffer.WithSize(5).
    // buffer will be flushed every second, regardless of
    // how many items were pushed
    buffer.WithFlushInterval(time.Second).
    // call this function when the buffer needs flushing
    buffer.WithFlusher(buffer.FlusherFunc(func(items []interface{}) {
      for _, item := range items {
        println(item.(string))
      }
    })).
    // Consume
    Consume()

  if err != nil {
    panic(err)
  }

  defer buff.Close()

  buff.Push("item 1")
  buff.Push("item 2")
  buff.Push("item 3")

  // block the current goroutine
  time.Sleep(3 * time.Second)

  println("done")
}
```

### Manual flush

```golang
package main

import (
  "time"

  "github.com/omniboost/go-buffer"
)

func main() {
  buff := buffer.New[any]().
    // buffer can hold up to 5 items
    buffer.WithSize(5).
    // call this function when the buffer needs flushing
    buffer.WithFlusher(buffer.FlusherFunc(func(items []interface{}) {
      for _, item := range items {
        println(item.(string))
      }
    })).
    // Consume
    Consume()

  if err != nil {
    panic(err)
  }

  defer buff.Close()

  buff.Push("item 1")
  buff.Push("item 2")
  buff.Push("item 3")

  // block the current goroutine
  time.Sleep(3*time.Second)

  buff.Flush()
  println("done")
}
```

## Documentation

Visit [Pkg.go.dev](https://pkg.go.dev/github.com/omniboost/go-buffer) for full documentation.

## License

[MIT License](https://github.com/omniboost/go-buffer/blob/master/LICENSE)
