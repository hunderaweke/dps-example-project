package events

// FeeTopic carries the events of Fee (dps_fee): quotes and rule sets.
// The names mirror dps-contracts proto/dps/fee/v1/events.proto.
const FeeTopic = "fee.events"

const (
	// FeeQuoteIssuedV1 is dps.fee.v1.QuoteIssued.
	FeeQuoteIssuedV1 Name = "fee.quote.issued.v1"
	// FeeRuleSetPublishedV1 is dps.fee.v1.RuleSetPublished.
	FeeRuleSetPublishedV1 Name = "fee.rule_set.published.v1"
)

var feeDomain = Domain{
	Name:  "fee",
	Topic: FeeTopic,
	Events: []Name{
		FeeQuoteIssuedV1,
		FeeRuleSetPublishedV1,
	},
}
