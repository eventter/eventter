package consumer

import (
	"bytes"
	"strconv"
	"testing"

	"eventter.io/mq/client"
)

func TestGroup_Offer(t *testing.T) {
	g, err := NewGroup(8)
	if err != nil {
		t.Fatal(err)
	}

	go func() {
		for i := 1; i <= 50; i++ {
			if err := g.Offer(&Message{Message: &client.Message{Data: []byte(strconv.Itoa(i))}}); err != nil {
				t.Fatal(err)
			}
		}
		g.Close()
	}()

	subscription := g.Subscribe()
	defer subscription.Close()

	for i := 1; ; i++ {
		m, err := subscription.Next()
		if err == ErrGroupClosed {
			break
		}

		expected := []byte(strconv.Itoa(i))
		if !bytes.Equal(expected, m.Message.Data) {
			t.Fatalf("expected %s, got %s", expected, m.Message.Data)
		}
	}
}

func TestGroup_Subscribe(t *testing.T) {
	g, err := NewGroup(8)
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	for i := 1; i < 10; i++ {
		subscription := g.Subscribe()
		subscription.Close()
		if subscription.ID != uint64(i) {
			t.Fatalf("expected subscription ID %d, got %d", i, subscription.ID)
		}
	}
}
