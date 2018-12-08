package consumers

import (
	"strconv"
	"testing"

	"eventter.io/mq/client"
)

func TestSubscription_Next(t *testing.T) {
	n := 8

	g, err := NewGroup(n)
	if err != nil {
		t.Fatal(err)
	}

	for i := 1; i <= n; i++ {
		if err := g.Offer(&Message{Message: &client.Message{Data: []byte(strconv.Itoa(i))}}); err != nil {
			t.Fatal(err)
		}
	}

	s := g.Subscribe()
	g.Close()

	defer s.Close()

	for {
		m, err := s.Next()
		if err == ErrGroupClosed {
			break
		} else if err != nil {
			t.Fatal(err)
		}

		if m.SeqNo < 1 {
			t.Fatalf("expected positive seq no, got %d", m.SeqNo)
		}

		if err := s.Ack(m.SeqNo); err != nil {
			t.Fatal(err)
		}
	}
}

func TestSubscription_Ack(t *testing.T) {
	g, err := NewGroup(8)
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	if err := g.Offer(&Message{Message: &client.Message{Data: []byte("1")}}); err != nil {
		t.Fatal(err)
	}
	if err := g.Offer(&Message{Message: &client.Message{Data: []byte("2")}}); err != nil {
		t.Fatal(err)
	}
	if err := g.Offer(&Message{Message: &client.Message{Data: []byte("3")}}); err != nil {
		t.Fatal(err)
	}

	s1 := g.Subscribe()
	defer s1.Close()
	s2 := g.Subscribe()
	defer s2.Close()

	m1, err := s1.Next()
	if err != nil {
		t.Fatal(err)
	}
	if got := string(m1.Message.Data); got != "1" {
		t.Fatalf("expected %s, got %s", "1", got)
	}
	if m1.SeqNo != 1 {
		t.Fatalf("expected %d, got %d", 1, m1.SeqNo)
	}
	if g.read != 0 {
		t.Fatalf("expected read to point to %d, got %d", 0, g.read)
	}

	m2, err := s2.Next()
	if err != nil {
		t.Fatal(err)
	}
	if got := string(m2.Message.Data); got != "2" {
		t.Fatalf("expected %s, got %s", "2", got)
	}
	if m2.SeqNo != 1 {
		t.Fatalf("expected %d, got %d", 1, m2.SeqNo)
	}
	if g.read != 0 {
		t.Fatalf("expected read to point to %d, got %d", 0, g.read)
	}

	if err := s2.Ack(m2.SeqNo); err != nil {
		t.Fatal(err)
	}
	if g.read != 0 {
		t.Fatalf("expected read to point to %d, got %d", 0, g.read)
	}

	if err := s1.Ack(m1.SeqNo); err != nil {
		t.Fatal(err)
	}
	if g.read != 2 {
		t.Fatalf("expected read to point to %d, got %d", 2, g.read)
	}

	s3 := g.Subscribe()
	defer s3.Close()

	m3, err := s3.Next()
	if err != nil {
		t.Fatal(err)
	}
	if got := string(m3.Message.Data); got != "3" {
		t.Fatalf("expected %s, got %s", "3", got)
	}
	if m3.SeqNo != 1 {
		t.Fatalf("expected %d, got %d", 1, m3.SeqNo)
	}
}

func TestSubscription_Nack(t *testing.T) {
	g, err := NewGroup(8)
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	if err := g.Offer(&Message{Message: &client.Message{Data: []byte("1")}}); err != nil {
		t.Fatal(err)
	}
	if err := g.Offer(&Message{Message: &client.Message{Data: []byte("2")}}); err != nil {
		t.Fatal(err)
	}
	if err := g.Offer(&Message{Message: &client.Message{Data: []byte("3")}}); err != nil {
		t.Fatal(err)
	}

	s1 := g.Subscribe()
	defer s1.Close()
	s2 := g.Subscribe()
	defer s2.Close()
	s3 := g.Subscribe()
	defer s3.Close()

	m1, err := s1.Next()
	if err != nil {
		t.Fatal(err)
	}
	if g.read != 0 {
		t.Fatalf("expected read to point to %d, got %d", 0, g.read)
	}

	m2, err := s2.Next()
	if err != nil {
		t.Fatal(err)
	}
	if g.read != 0 {
		t.Fatalf("expected read to point to %d, got %d", 0, g.read)
	}

	if err := s2.Nack(m2.SeqNo); err != nil {
		t.Fatal(err)
	}
	if g.read != 0 {
		t.Fatalf("expected read to point to %d, got %d", 0, g.read)
	}

	if err := s1.Nack(m1.SeqNo); err != nil {
		t.Fatal(err)
	}
	if g.read != 0 {
		t.Fatalf("expected read to point to %d, got %d", 0, g.read)
	}

	m3, err := s3.Next()
	if err != nil {
		t.Fatal(err)
	}
	if got := string(m3.Message.Data); got != "1" {
		t.Fatalf("expected %s, got %s", "1", got)
	}
	if m3.SeqNo != 1 {
		t.Fatalf("expected %d, got %d", 1, m3.SeqNo)
	}
	if g.read != 0 {
		t.Fatalf("expected read to point to %d, got %d", 0, g.read)
	}
}
