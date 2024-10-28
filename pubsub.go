package gopubsub

import (
	"fmt"
	"sync"

	"github.com/google/uuid"
)

func New() PubSub {
	pub := publisher{}
	return &pub
}

type Topic string

type PubSub interface {
	Publisher
	Subscriber
}

type Publisher interface {
	Create(Topic)
	Publish(Topic, any) error
}

type Subscriber interface {
	Subscribe(Topic) (uuid.UUID, <-chan any, error)
	Unsubscribe(Topic, uuid.UUID) error
}

type publisher struct {
	topic2sub sync.Map
	mu        sync.Mutex
}

func (p *publisher) Subscribe(topic Topic) (uuid.UUID, <-chan any, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	id := uuid.New()
	v, ok := p.topic2sub.Load(topic)
	if !ok {
		return uuid.UUID{}, nil, fmt.Errorf("topic %v: %w", topic, ErrNotFound)
	}

	uuid2chan, ok := v.(*sync.Map)
	if !ok {
		return uuid.UUID{}, nil, fmt.Errorf("topic %v: %w", topic, ErrNotFound)
	}
	anyChan := make(chan any)
	uuid2chan.Store(id, anyChan)

	return id, anyChan, nil
}
func (p *publisher) Unsubscribe(topic Topic, id uuid.UUID) error {
	v, ok := p.topic2sub.Load(topic)
	if !ok {
		return fmt.Errorf("topic %v: %w", topic, ErrNotFound)
	}
	uuid2chan := v.(*sync.Map)
	v, ok = uuid2chan.Load(id)
	if !ok {
		return fmt.Errorf("id %v: %w", id, ErrNotFound)
	}
	anyChan, ok := v.(chan any)
	done := make(chan struct{})
	if ok {
		defer close(anyChan)
		go func() {
			for {
				select {
				case <-anyChan:
				case <-done:
					return
				}
			}
		}()
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	uuid2chan.Delete(id)
	close(done)
	return nil
}

func (p *publisher) Create(topic Topic) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.topic2sub.Store(topic, &sync.Map{})
}

func (p *publisher) Publish(topic Topic, data any) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	v, ok := p.topic2sub.Load(topic)
	if !ok {
		return fmt.Errorf("topic %v: %w", topic, ErrNotFound)
	}
	uuid2chan, ok := v.(*sync.Map)
	if !ok {
		return fmt.Errorf("topic %v: %w", topic, ErrNotFound)
	}
	uuid2chan.Range(func(key, value any) bool {
		anyChan, ok := value.(chan any)
		if !ok {
			return false
		}
		anyChan <- data
		return true
	})
	return nil
}
