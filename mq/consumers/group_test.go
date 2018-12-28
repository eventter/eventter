package consumers

import (
	"fmt"
	"strconv"
	"sync"
	"testing"
	"unsafe"

	"eventter.io/mq/emq"
)

func TestGroup_Offer(t *testing.T) {
	g, err := NewGroup(8)
	if err != nil {
		t.Fatal(err)
	}

	go func() {
		for i := 1; i <= 50; i++ {
			if err := g.Offer(&Message{Message: &emq.Message{Data: []byte(strconv.Itoa(i))}}); err != nil {
				t.Fatal(err)
			}
		}
		g.Close()
	}()

	subscription := g.Subscribe()
	defer subscription.Close()

	max := 0
	for i := 1; ; i++ {
		m, err := subscription.Next()
		if err == ErrGroupClosed {
			break
		}

		if strconv.Itoa(i) != string(m.Message.Data) {
			t.Fatalf("expected %d, got %s", i, m.Message.Data)
		}

		if err := subscription.Ack(m.SeqNo); err != nil {
			t.Fatal(err)
		}

		max = i
	}

	if max != 50 {
		t.Fatalf("expected %d, got %d", 50, max)
	}
}

func TestGroup_Subscribe(t *testing.T) {
	currentSubscriptionID = 0

	g, err := NewGroup(8)
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	for i := 1; i < 10; i++ {
		subscription := g.Subscribe()
		if err := subscription.Close(); err != nil {
			t.Fatal(err)
		}
		if subscription.ID != uint64(i) {
			t.Fatalf("expected subscription ID %d, got %d", i, subscription.ID)
		}
	}
}

func BenchmarkGroup(b *testing.B) {
	oneMBOfMemory := 1024 * 1024 / int(unsafe.Sizeof(Message{}))

	benchmarks := []struct {
		size      int
		producers int
		consumers int
	}{
		{64, 1, 1},
		{64, 1, 2},
		{64, 1, 5},
		{64, 1, 10},
		{64, 2, 1},
		{64, 2, 2},
		{64, 2, 5},
		{64, 2, 10},
		{64, 5, 1},
		{64, 5, 2},
		{64, 5, 5},
		{64, 5, 10},
		{64, 10, 1},
		{64, 10, 2},
		{64, 10, 5},
		{64, 10, 10},
		{1024, 1, 1},
		{1024, 1, 2},
		{1024, 1, 5},
		{1024, 1, 10},
		{1024, 2, 1},
		{1024, 2, 2},
		{1024, 2, 5},
		{1024, 2, 10},
		{1024, 5, 1},
		{1024, 5, 2},
		{1024, 5, 5},
		{1024, 5, 10},
		{1024, 10, 1},
		{1024, 10, 2},
		{1024, 10, 5},
		{1024, 10, 10},
		{oneMBOfMemory, 1, 1},
		{oneMBOfMemory, 1, 2},
		{oneMBOfMemory, 1, 5},
		{oneMBOfMemory, 1, 10},
		{oneMBOfMemory, 2, 1},
		{oneMBOfMemory, 2, 2},
		{oneMBOfMemory, 2, 5},
		{oneMBOfMemory, 2, 10},
		{oneMBOfMemory, 5, 1},
		{oneMBOfMemory, 5, 2},
		{oneMBOfMemory, 5, 5},
		{oneMBOfMemory, 5, 10},
		{oneMBOfMemory, 10, 1},
		{oneMBOfMemory, 10, 2},
		{oneMBOfMemory, 10, 5},
		{oneMBOfMemory, 10, 10},
		{65536, 1, 1},
		{65536, 1, 2},
		{65536, 1, 5},
		{65536, 1, 10},
		{65536, 2, 1},
		{65536, 2, 2},
		{65536, 2, 5},
		{65536, 2, 10},
		{65536, 5, 1},
		{65536, 5, 2},
		{65536, 5, 5},
		{65536, 5, 10},
		{65536, 10, 1},
		{65536, 10, 2},
		{65536, 10, 5},
		{65536, 10, 10},
		{131072, 1, 1},
		{131072, 1, 2},
		{131072, 1, 5},
		{131072, 1, 10},
		{131072, 2, 1},
		{131072, 2, 2},
		{131072, 2, 5},
		{131072, 2, 10},
		{131072, 5, 1},
		{131072, 5, 2},
		{131072, 5, 5},
		{131072, 5, 10},
		{131072, 10, 1},
		{131072, 10, 2},
		{131072, 10, 5},
		{131072, 10, 10},
	}

	for _, bm := range benchmarks {
		b.Run(fmt.Sprintf("size=%d/producers=%d/consumers=%d", bm.size, bm.producers, bm.consumers), func(b *testing.B) {
			benchmarkGroup(b, bm.size, b.N, bm.producers, bm.consumers)
		})
	}
}

func benchmarkGroup(b *testing.B, size int, messages int, producers int, consumers int) {
	messagesPerProducer := allocate(messages, producers)
	messagesPerConsumer := allocate(messages, consumers)

	b.ResetTimer()

	g, err := NewGroup(size)
	if err != nil {
		b.Fatal(err)
	}

	produceWg := sync.WaitGroup{}
	for i := 0; i < producers; i++ {
		produceWg.Add(1)
		go func(i int) {
			defer produceWg.Done()

			for j := 0; j < messagesPerProducer[i]; j++ {
				m := &Message{Message: &emq.Message{Data: []byte(".")}}
				if err := g.Offer(m); err != nil {
					b.Fatal(err)
				}
			}
		}(i)
	}

	consumeWg := sync.WaitGroup{}
	for i := 0; i < consumers; i++ {
		consumeWg.Add(1)
		go func(i int) {
			defer consumeWg.Done()

			subscription := g.Subscribe()
			defer subscription.Close()

			for j := 0; j < messagesPerConsumer[i]; j++ {
				m, err := subscription.Next()
				if err != nil {
					b.Fatal(err)
				}

				if err := subscription.Ack(m.SeqNo); err != nil {
					b.Fatal(err)
				}
			}
		}(i)
	}

	produceWg.Wait()
	g.Close()
	consumeWg.Wait()
}

func allocate(n int, parts int) []int {
	allocations := make([]int, parts)
	for i := 0; i < parts; i++ {
		allocations[i] = n / parts
	}
	n = n % parts
	for i := 0; n > 0; i++ {
		allocations[i]++
		n--
	}
	return allocations
}
