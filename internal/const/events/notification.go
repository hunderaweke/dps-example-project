package events

// NotificationTopic carries the events of Notification (dps_notification): deliveries and preferences.
// The names mirror dps-contracts proto/dps/notification/v1/events.proto.
const NotificationTopic = "notification.events"

const (
	// NotificationMessageDeliveredV1 is dps.notification.v1.MessageDelivered.
	NotificationMessageDeliveredV1 Name = "notification.message.delivered.v1"
	// NotificationMessageFailedV1 is dps.notification.v1.MessageFailed.
	NotificationMessageFailedV1 Name = "notification.message.failed.v1"
	// NotificationMessageReadV1 is dps.notification.v1.MessageRead.
	NotificationMessageReadV1 Name = "notification.message.read.v1"
	// NotificationPreferenceUpdatedV1 is dps.notification.v1.PreferenceUpdated.
	NotificationPreferenceUpdatedV1 Name = "notification.preference.updated.v1"
)

var notificationDomain = Domain{
	Name:  "notification",
	Topic: NotificationTopic,
	Events: []Name{
		NotificationMessageDeliveredV1,
		NotificationMessageFailedV1,
		NotificationMessageReadV1,
		NotificationPreferenceUpdatedV1,
	},
}
