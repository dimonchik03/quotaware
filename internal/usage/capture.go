package usage

import (
	"bytes"
	"errors"
	"io"
	"sync"
)

type Capture struct {
	io.ReadCloser
	limit    int64
	mu       sync.Mutex
	buffer   bytes.Buffer
	overflow bool
	done     func(body []byte, complete bool)
	once     sync.Once
}

func NewCapture(body io.ReadCloser, limit int64, done func(body []byte, complete bool)) *Capture {
	return &Capture{ReadCloser: body, limit: limit, done: done}
}

func (c *Capture) Read(p []byte) (int, error) {
	n, err := c.ReadCloser.Read(p)
	if n > 0 {
		c.mu.Lock()
		if !c.overflow {
			if int64(c.buffer.Len()+n) > c.limit {
				c.overflow = true
				c.buffer.Reset()
			} else {
				c.buffer.Write(p[:n])
			}
		}
		c.mu.Unlock()
	}
	if errors.Is(err, io.EOF) {
		c.finish(true)
	}
	return n, err
}

func (c *Capture) Close() error {
	err := c.ReadCloser.Close()
	c.finish(false)
	return err
}

func (c *Capture) finish(complete bool) {
	c.once.Do(func() {
		c.mu.Lock()
		body := append([]byte(nil), c.buffer.Bytes()...)
		if c.overflow {
			body = nil
		}
		c.mu.Unlock()
		c.done(body, complete)
	})
}
