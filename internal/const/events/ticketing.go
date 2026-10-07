package events

// TicketingTopic carries the events of Event Ticketing (dps_ticketing): events, tickets, refunds.
// The names mirror dps-contracts proto/dps/ticketing/v1/events.proto.
const TicketingTopic = "ticketing.events"

const (
	// TicketingEventSubmittedV1 is dps.ticketing.v1.EventSubmitted.
	TicketingEventSubmittedV1 Name = "ticketing.event.submitted.v1"
	// TicketingEventPublishedV1 is dps.ticketing.v1.EventPublished.
	TicketingEventPublishedV1 Name = "ticketing.event.published.v1"
	// TicketingEventRejectedV1 is dps.ticketing.v1.EventRejected.
	TicketingEventRejectedV1 Name = "ticketing.event.rejected.v1"
	// TicketingTicketIssuedV1 is dps.ticketing.v1.TicketIssued.
	TicketingTicketIssuedV1 Name = "ticketing.ticket.issued.v1"
	// TicketingTicketUsedV1 is dps.ticketing.v1.TicketUsed.
	TicketingTicketUsedV1 Name = "ticketing.ticket.used.v1"
	// TicketingTicketScanRejectedV1 is dps.ticketing.v1.TicketScanRejected.
	TicketingTicketScanRejectedV1 Name = "ticketing.ticket.scan_rejected.v1"
	// TicketingRefundRequestedV1 is dps.ticketing.v1.RefundRequested.
	TicketingRefundRequestedV1 Name = "ticketing.refund.requested.v1"
	// TicketingRefundRejectedV1 is dps.ticketing.v1.RefundRejected.
	TicketingRefundRejectedV1 Name = "ticketing.refund.rejected.v1"
	// TicketingTicketRefundedV1 is dps.ticketing.v1.TicketRefunded.
	TicketingTicketRefundedV1 Name = "ticketing.ticket.refunded.v1"
)

var ticketingDomain = Domain{
	Name:  "ticketing",
	Topic: TicketingTopic,
	Events: []Name{
		TicketingEventSubmittedV1,
		TicketingEventPublishedV1,
		TicketingEventRejectedV1,
		TicketingTicketIssuedV1,
		TicketingTicketUsedV1,
		TicketingTicketScanRejectedV1,
		TicketingRefundRequestedV1,
		TicketingRefundRejectedV1,
		TicketingTicketRefundedV1,
	},
}
