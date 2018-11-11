package msgid

import (
	"encoding/hex"
	"math/rand"
	"testing"
	"time"
)

type staticTimeSource time.Time

func (s staticTimeSource) Now() time.Time {
	return time.Time(s)
}

var expectedTime, _ = time.Parse(time.RFC3339, "2018-11-04T23:09:00Z")

func TestGenerator_New(t *testing.T) {
	g := NewGenerator(staticTimeSource(expectedTime), rand.NewSource(1))
	g.New()
}

func TestStdTimeSource_Now(t *testing.T) {
	src := NewStdTimeSource()

	before := time.Now()
	ts := src.Now()
	after := time.Now()

	if before.After(ts) {
		t.Error("expected to be after before")
	}
	if after.Before(ts) {
		t.Error("expected to be before after")
	}
}

func TestID_Bytes(t *testing.T) {
	g := NewGenerator(staticTimeSource(expectedTime), rand.NewSource(1))

	id := g.New()

	expected := "0166e0fc8ee0044278629a0f5f3f164f"
	if got := hex.EncodeToString(id.Bytes()); got != expected {
		t.Errorf("hex - expected: %s, got: %s", expected, got)
	}
}

func TestID_Randomness(t *testing.T) {
	g := NewGenerator(staticTimeSource(expectedTime), rand.NewSource(1))

	id := g.New()

	expected := "044278629a0f5f3f164f"
	if got := hex.EncodeToString(id.Randomness()); got != expected {
		t.Errorf("hex - expected: %s, got: %s", expected, got)
	}
}

func TestID_Time(t *testing.T) {
	g := NewGenerator(staticTimeSource(expectedTime), rand.NewSource(1))

	id := g.New()

	if gotTime := id.Time(); gotTime.In(expectedTime.Location()) != expectedTime {
		t.Errorf("time - expected: %s, got: %s", expectedTime, gotTime)
	}
}

func TestID_FromBytes(t *testing.T) {
	g := NewGenerator(NewStdTimeSource(), rand.NewSource(1))

	id := g.New()

	newID := ID{}
	newID.FromBytes(id.Bytes())

	if newID != id {
		t.Error("ids should be same")
	}
}
