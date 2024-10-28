package gopubsub_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	gopubsub "github.com/mv-kan/go-pubsub"
	"github.com/stretchr/testify/assert"
)

func TestPubSub_Ok(t *testing.T) {
	pubsub := gopubsub.New()
	var (
		pub gopubsub.Publisher  = pubsub
		sub gopubsub.Subscriber = pubsub
	)
	theTopic := gopubsub.Topic("mytopic")
	pub.Create(theTopic)

	testData := 1
	// nothing happens, no subs
	pub.Publish(theTopic, testData)

	subID, anyChan, err := sub.Subscribe(theTopic)
	assert.Nil(t, err)
	assert.NotNil(t, anyChan)
	data := 0
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		tmp := <-anyChan
		data = tmp.(int)
		wg.Done()
	}()
	pub.Publish(theTopic, testData)
	wg.Wait()
	sub.Unsubscribe(theTopic, subID)

	assert.Equal(t, testData, data)

	// nothing should happen, no subs
	pub.Publish(theTopic, testData)
}

func TestStress(t *testing.T) {
	pubsub := gopubsub.New()
	var (
		pub gopubsub.Publisher  = pubsub
		sub gopubsub.Subscriber = pubsub
	)
	theTopic := gopubsub.Topic("mytopic")
	pub.Create(theTopic)

	// nothing happens, no subs
	pub.Publish(theTopic, 0)

	subIDs := []uuid.UUID{}
	anyChans := []<-chan any{}

	numOfSubs := 2000

	for i := 0; i < numOfSubs; i++ {
		subID, anyChan, err := sub.Subscribe(theTopic)
		assert.Nil(t, err)
		assert.NotNil(t, anyChan)
		subIDs = append(subIDs, subID)
		anyChans = append(anyChans, anyChan)
	}
	wg := sync.WaitGroup{}

	numOfMsgs := numOfSubs
	msgHandling := func(id uuid.UUID, anyChan <-chan any, unsubWhen int) {
		defer wg.Done()
		i := 0
		for {
			a := <-anyChan
			n := a.(int)
			assert.Equal(t, i, n)
			i++
			if i == unsubWhen || i == numOfMsgs {
				before := time.Now()
				sub.Unsubscribe(theTopic, id)
				fmt.Printf("Ubsub id=%v, unsubbing took %v\n", id, time.Since(before))
				return
			}
		}
	}
	// run msg handlers
	wg.Add(numOfSubs)
	for i := 0; i < numOfSubs; i++ {
		if i%2 == 0 {
			go msgHandling(subIDs[i], anyChans[i], i)
		} else {
			go msgHandling(subIDs[i], anyChans[i], numOfMsgs)
		}
	}

	for i := 0; i < numOfMsgs; i++ {
		before := time.Now()
		pub.Publish(theTopic, i)
		fmt.Printf("msg n %d, publishing took %v\n", i, time.Since(before))
	}
	wg.Wait()

	pub.Publish(theTopic, 0)
}

func TestWait_Ok(t *testing.T) {
	ch := make(chan any)
	testData := 1
	go func() {
		time.Sleep(time.Millisecond)
		ch <- testData
	}()
	data, err := gopubsub.Wait(ch, time.Millisecond*10)
	n := data.(int)
	assert.Nil(t, err)
	assert.Equal(t, testData, n)
}
func TestWait_NoData(t *testing.T) {
	ch := make(chan any)
	_, err := gopubsub.Wait(ch, time.Millisecond*10)
	assert.ErrorIs(t, err, gopubsub.ErrTimeout)
}
