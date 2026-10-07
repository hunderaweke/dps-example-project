package events

// UtilityTopic carries the events of Utility Payment (dps_utility): bill inquiries and payments through aggregators.
// The names mirror dps-contracts proto/dps/utility/v1/events.proto.
const UtilityTopic = "utility.events"

const (
	// UtilityBillInquiredV1 is dps.utility.v1.BillInquired.
	UtilityBillInquiredV1 Name = "utility.bill.inquired.v1"
	// UtilityBillPaymentCompletedV1 is dps.utility.v1.BillPaymentCompleted.
	UtilityBillPaymentCompletedV1 Name = "utility.bill_payment.completed.v1"
	// UtilityBillPaymentFailedV1 is dps.utility.v1.BillPaymentFailed.
	UtilityBillPaymentFailedV1 Name = "utility.bill_payment.failed.v1"
	// UtilityRoutingUpdatedV1 is dps.utility.v1.RoutingUpdated.
	UtilityRoutingUpdatedV1 Name = "utility.routing.updated.v1"
)

var utilityDomain = Domain{
	Name:  "utility",
	Topic: UtilityTopic,
	Events: []Name{
		UtilityBillInquiredV1,
		UtilityBillPaymentCompletedV1,
		UtilityBillPaymentFailedV1,
		UtilityRoutingUpdatedV1,
	},
}
