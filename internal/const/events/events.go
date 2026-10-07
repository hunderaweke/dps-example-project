// Package events is the catalog of DPS domain events the audit service consumes.
// The source of truth is the dps-contracts repo: every name here is the
// (dps.events.v1.event).type of a message in proto/dps/<domain>/v1/events.proto.
//
// Event names follow domain.entity.event.vN: lowercase, the event in the past
// tense, multi-word segments in snake_case. domain is the producing service's
// namespace, the same one its error codes use. A breaking change adds a
// dps.<domain>.v2 package and .v2 names, published alongside v1.
//
// Each domain publishes every event on one topic, <domain>.events, keyed by
// aggregate ID. The event name travels in the HeaderEventType record header.
package events

import (
	"slices"
	"strings"
)

const (
	// AuditConsumerGroup is the consumer group of the audit service.
	AuditConsumerGroup = "audit.universal"
	// HeaderEventType is the Kafka record header that carries the event Name.
	HeaderEventType = "x-dps-event-type"
)

// Name is a fully qualified event name, domain.entity.event.vN.
type Name string

func (n Name) String() string { return string(n) }

// Domain returns the first segment of the name.
func (n Name) Domain() string {
	d, _, _ := strings.Cut(string(n), ".")
	return d
}

// Domain groups the events one producer publishes on its topic.
type Domain struct {
	Name   string
	Topic  string
	Events []Name
}

var domains = []Domain{
	authDomain,
	onboardingDomain,
	accountDomain,
	paymentDomain,
	tpiDomain,
	ledgerDomain,
	feeDomain,
	airtimeDomain,
	utilityDomain,
	airlineDomain,
	notificationDomain,
	adminopsDomain,
	fuelDomain,
	lendingDomain,
	assistantDomain,
	ticketingDomain,
	dwhDomain,
}

// Domains returns a copy of every domain in the catalog.
func Domains() []Domain {
	out := make([]Domain, len(domains))
	for i, d := range domains {
		d.Events = slices.Clone(d.Events)
		out[i] = d
	}
	return out
}

// All returns every event name in the catalog.
func All() []Name {
	var out []Name
	for _, d := range domains {
		out = append(out, d.Events...)
	}
	return out
}

// Topics returns the default topic of every domain.
func Topics() []string {
	out := make([]string, len(domains))
	for i, d := range domains {
		out[i] = d.Topic
	}
	return out
}
