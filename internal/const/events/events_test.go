package events_test

import (
	"regexp"
	"testing"

	"github.com/hunderaweke/dps-audit-service/internal/const/events"
)

var namePattern = regexp.MustCompile(`^[a-z]+(\.[a-z]+(_[a-z]+)*){2}\.v[1-9][0-9]*$`)

func TestCatalog(t *testing.T) {
	seenNames := map[events.Name]bool{}
	seenTopics := map[string]bool{}

	for _, d := range events.Domains() {
		if d.Topic != d.Name+".events" {
			t.Errorf("domain %q: topic %q, want %q", d.Name, d.Topic, d.Name+".events")
		}
		if seenTopics[d.Topic] {
			t.Errorf("duplicate topic %q", d.Topic)
		}
		seenTopics[d.Topic] = true

		if len(d.Events) == 0 {
			t.Errorf("domain %q has no events", d.Name)
		}
		for _, n := range d.Events {
			if !namePattern.MatchString(n.String()) {
				t.Errorf("%q does not match domain.entity.event.vN", n)
			}
			if n.Domain() != d.Name {
				t.Errorf("%q is listed under domain %q", n, d.Name)
			}
			if seenNames[n] {
				t.Errorf("duplicate event %q", n)
			}
			seenNames[n] = true
		}
	}

	if got, want := len(events.All()), len(seenNames); got != want {
		t.Errorf("All() returned %d names, want %d", got, want)
	}
	if got, want := len(events.Topics()), len(seenTopics); got != want {
		t.Errorf("Topics() returned %d topics, want %d", got, want)
	}
}

func TestDomainsReturnsCopy(t *testing.T) {
	d := events.Domains()
	d[0].Events[0] = "tampered.name.here.v1"

	if events.Domains()[0].Events[0] == "tampered.name.here.v1" {
		t.Fatal("Domains() exposes the package catalog")
	}
}
