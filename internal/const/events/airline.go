package events

// AirlineTopic carries the events of ET Airlines (dps_airline): bookings, tickets, refunds.
// The names mirror dps-contracts proto/dps/airline/v1/events.proto.
const AirlineTopic = "airline.events"

const (
	// AirlineBookingHeldV1 is dps.airline.v1.BookingHeld.
	AirlineBookingHeldV1 Name = "airline.booking.held.v1"
	// AirlineBookingTicketedV1 is dps.airline.v1.BookingTicketed.
	AirlineBookingTicketedV1 Name = "airline.booking.ticketed.v1"
	// AirlineBookingCancelledV1 is dps.airline.v1.BookingCancelled.
	AirlineBookingCancelledV1 Name = "airline.booking.cancelled.v1"
	// AirlineBookingTicketingFailedV1 is dps.airline.v1.BookingTicketingFailed.
	AirlineBookingTicketingFailedV1 Name = "airline.booking.ticketing_failed.v1"
	// AirlineRefundRequestedV1 is dps.airline.v1.RefundRequested.
	AirlineRefundRequestedV1 Name = "airline.refund.requested.v1"
	// AirlineRefundCompletedV1 is dps.airline.v1.RefundCompleted.
	AirlineRefundCompletedV1 Name = "airline.refund.completed.v1"
	// AirlineRefundFailedV1 is dps.airline.v1.RefundFailed.
	AirlineRefundFailedV1 Name = "airline.refund.failed.v1"
)

var airlineDomain = Domain{
	Name:  "airline",
	Topic: AirlineTopic,
	Events: []Name{
		AirlineBookingHeldV1,
		AirlineBookingTicketedV1,
		AirlineBookingCancelledV1,
		AirlineBookingTicketingFailedV1,
		AirlineRefundRequestedV1,
		AirlineRefundCompletedV1,
		AirlineRefundFailedV1,
	},
}
