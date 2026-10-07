package events

// FuelTopic carries the events of Fuel (dps_fuel): fuel purchases.
// The names mirror dps-contracts proto/dps/fuel/v1/events.proto.
const FuelTopic = "fuel.events"

const (
	// FuelPurchaseConfirmedV1 is dps.fuel.v1.PurchaseConfirmed.
	FuelPurchaseConfirmedV1 Name = "fuel.purchase.confirmed.v1"
	// FuelPurchaseExpiredV1 is dps.fuel.v1.PurchaseExpired.
	FuelPurchaseExpiredV1 Name = "fuel.purchase.expired.v1"
)

var fuelDomain = Domain{
	Name:  "fuel",
	Topic: FuelTopic,
	Events: []Name{
		FuelPurchaseConfirmedV1,
		FuelPurchaseExpiredV1,
	},
}
