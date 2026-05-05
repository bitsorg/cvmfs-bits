package fakecas

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"cvmfs.io/prepub/pkg/observe"
)

type CAS struct {
	objects    map[string][]byte
	mu         sync.RWMutex
	PutLatency time.Duration
	PutFailRate float64
	obs        *observe.Provider
}

func New(obs *observe.Provider) *CAS {
	return &CAS{
		objects: make(map[string][]byte),
		obs:     obs,
	}
}

func (c *CAS) Exists(ctx context.Context, hash string) (bool, error) {
	ctx, span := c.obs.Tracer.Start(ctx, "fakecas.exists")
	defer span.End()

	c.mu.RLock()
	defer c.mu.RUnlock()
	_, ok := c.objects[hash]
	return ok, nil
}

func (c *CAS) Put(ctx context.Context, hash string, r io.Reader, size int64) error {
	ctx, span := c.obs.Tracer.Start(ctx, "fakecas.put")
	defer span.End()

	if c.PutLatency > 0 {
		time.Sleep(c.PutLatency)
	}

	var data []byte
	if r != nil {
		var err error
		data, err = io.ReadAll(r)
		if err != nil {
			span.RecordError(err)
			return fmt.Errorf("reading data: %w", err)
		}
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	c.objects[hash] = data

	return nil
}

func (c *CAS) Size(ctx context.Context, hash string) (int64, error) {
	c.mu.RLock()
	data, ok := c.objects[hash]
	c.mu.RUnlock()
	if !ok {
		return 0, fmt.Errorf("object not found: %s", hash)
	}
	return int64(len(data)), nil
}

func (c *CAS) Get(ctx context.Context, hash string) (io.ReadCloser, error) {
	ctx, span := c.obs.Tracer.Start(ctx, "fakecas.get")
	defer span.End()

	c.mu.RLock()
	data, ok := c.objects[hash]
	c.mu.RUnlock()

	if !ok {
		span.RecordError(fmt.Errorf("not found"))
		return nil, fmt.Errorf("object not found: %s", hash)
	}

	return io.NopCloser(bytes.NewReader(data)), nil
}

func (c *CAS) Delete(ctx context.Context, hash string) error {
	ctx, span := c.obs.Tracer.Start(ctx, "fakecas.delete")
	defer span.End()

	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.objects, hash)

	return nil
}

func (c *CAS) List(ctx context.Context) ([]string, error) {
	ctx, span := c.obs.Tracer.Start(ctx, "fakecas.list")
	defer span.End()

	c.mu.RLock()
	defer c.mu.RUnlock()

	var hashes []string
	for hash := range c.objects {
		hashes = append(hashes, hash)
	}
	return hashes, nil
}

func (c *CAS) ObjectCount() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.objects)
}

func (c *CAS) TotalBytes() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var total int64
	for _, data := range c.objects {
		total += int64(len(data))
	}
	return total
}
