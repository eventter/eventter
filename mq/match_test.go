package mq

import (
	"fmt"
	"testing"
)

func TestRoutingKeyMatches(t *testing.T) {
	tests := []struct {
		pattern    string
		routingKey string
		expected   bool
	}{
		{"", "", true},
		{"", "foo", false},
		{"foo", "foo", true},
		{"foo", "foo.", false},
		{"foo", "bar", false},
		{"foo.bar", "foo.bar", true},
		{"foo.bar", "foo.qux", false},
		{"foo.bar", "baz.qux", false},
		{"foo.*", "foo", false},
		{"foo.*", "foo.bar", true},
		{"foo.*", "foo.bar.", false},
		{"foo.*", "foo.qux", true},
		{"foo.*", "foo.bar.baz", false},
		{"foo.*.bar", "foo", false},
		{"foo.*.bar", "foo.bar", false},
		{"foo.*.bar", "foo.bar.bar", true},
		{"foo.*.bar", "foo.qux.bar", true},
		{"foo.*.bar", "foo.bar.bar.qux", false},
		{"*.foo", "foo", false},
		{"*.foo", "foo.bar", false},
		{"*.foo", "bar.foo", true},
		{"*.foo", "baz.foo", true},
		{"*.foo", "qux.bar.foo", false},
		{"foo.#", "foo", true},
		{"foo.#", "bar", false},
		{"foo.#", "foo.bar", true},
		{"foo.#", "foo.bar.baz", true},
		{"foo.#.bar", "foo", false},
		{"foo.#.bar", "foo.bar", true},
		{"foo.#.bar", "foo.qux.bar", true},
		{"foo.#.bar", "foo.qux.xyz", false},
		{"foo.#.bar", "foo.qux.xyz.bar", true},
		{"#.foo", "foo", true},
		{"#.foo", "foo.bar", false},
		{"#.foo", "bar.foo", true},
		{"#.foo", "qux.bar.foo", true},
		{"#.foo", "bar.foo.baz", false},
		{"*.*", ".", false},
		{"*.*", "a.", false},
		{"*.*", "a.b", true},
		{"*.*", "a.b.", false},
		{"*.*", "a.b.c", false},
		{"*.*.*", "a.b.", false},
		{"#", "", true},
		{"#.#", "", true},
		{"#.#", "a", true},
		{"#.#", "a.", false},
		{"#.#", "a.b", true},
		{"#.#", "a.b.", false},
		{"#.#", "a.b.c", true},
		{"*.#", "", false},
		{"*.#", "foo", true},
		{"*.#", "foo.", false},
		{"*.#", "foo.bar", true},
		{"*.#", "foo.bar.", false},
		{"*.#", "foo.bar.baz", true},
		{"*.#", "foo.bar.baz.", false},
		{"#.*", "", false},
		{"#.*", "foo", true},
		{"#.*", "foo.", false},
		{"#.*", "foo.bar", true},
		{"#.*", "foo.bar.baz.qux", true},
		{"*.foo.*", "", false},
		{"*.foo.*", "x.foo", false},
		{"*.foo.*", "foo.y", false},
		{"*.foo.*", "x.foo.y", true},
		{"*.foo.*", "x.foo.y.", false},
		{"*.foo.*", "x.foo.y.bar", false},
		{"#.foo.*", "", false},
		{"#.foo.*", "foo", false},
		{"#.foo.*", "foo.bar", true},
		{"#.foo.*", "foo.bar.", false},
		{"#.foo.*", "a.foo.bar", true},
		{"#.foo.*", "a.b.foo.c", true},
		{"*.foo.#", "", false},
		{"*.foo.#", "foo", false},
		{"*.foo.#", "x.foo", true},
		{"*.foo.#", "x.foo.", false},
		{"*.foo.#", "a.foo.bar", true},
		{"*.foo.#", "a.b.foo.c", false},
	}
	for _, test := range tests {
		t.Run(fmt.Sprintf("match %q by pattern %q", test.routingKey, test.pattern), func(t *testing.T) {
			if got := routingKeyMatches(test.pattern, test.routingKey); got != test.expected {
				t.Errorf("expected %v, got %v", test.expected, got)
			}
		})
	}
}
