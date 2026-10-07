package events

// TPITopic carries the events of Third-Party Integration (dps_tpi): EthSwitch and partner rails.
// The names mirror dps-contracts proto/dps/tpi/v1/events.proto.
const TPITopic = "tpi.events"

const (
	// TPIRequestTimedOutV1 is dps.tpi.v1.RequestTimedOut.
	TPIRequestTimedOutV1 Name = "tpi.request.timed_out.v1"
	// TPIStatusResolvedV1 is dps.tpi.v1.StatusResolved.
	TPIStatusResolvedV1 Name = "tpi.status.resolved.v1"
	// TPIWebhookRejectedV1 is dps.tpi.v1.WebhookRejected.
	TPIWebhookRejectedV1 Name = "tpi.webhook.rejected.v1"
)

var tpiDomain = Domain{
	Name:  "tpi",
	Topic: TPITopic,
	Events: []Name{
		TPIRequestTimedOutV1,
		TPIStatusResolvedV1,
		TPIWebhookRejectedV1,
	},
}
