package events

// AirtimeTopic carries the events of Airtime (dps_airtime): top-up orders.
// The names mirror dps-contracts proto/dps/airtime/v1/events.proto.
const AirtimeTopic = "airtime.events"

const (
	// AirtimeOrderPlacedV1 is dps.airtime.v1.OrderPlaced.
	AirtimeOrderPlacedV1 Name = "airtime.order.placed.v1"
	// AirtimeOrderCompletedV1 is dps.airtime.v1.OrderCompleted.
	AirtimeOrderCompletedV1 Name = "airtime.order.completed.v1"
	// AirtimeOrderFailedV1 is dps.airtime.v1.OrderFailed.
	AirtimeOrderFailedV1 Name = "airtime.order.failed.v1"
	// AirtimeOrderOutcomeUnknownV1 is dps.airtime.v1.OrderOutcomeUnknown.
	AirtimeOrderOutcomeUnknownV1 Name = "airtime.order.outcome_unknown.v1"
)

var airtimeDomain = Domain{
	Name:  "airtime",
	Topic: AirtimeTopic,
	Events: []Name{
		AirtimeOrderPlacedV1,
		AirtimeOrderCompletedV1,
		AirtimeOrderFailedV1,
		AirtimeOrderOutcomeUnknownV1,
	},
}
