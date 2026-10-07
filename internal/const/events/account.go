package events

// AccountTopic carries the events of Account Management (dps_account): customers, accounts, product catalogue.
// The names mirror dps-contracts proto/dps/account/v1/events.proto.
const AccountTopic = "account.events"

const (
	// AccountAccountOpenedV1 is dps.account.v1.AccountOpened.
	AccountAccountOpenedV1 Name = "account.account.opened.v1"
	// AccountAccountStatusChangedV1 is dps.account.v1.AccountStatusChanged.
	AccountAccountStatusChangedV1 Name = "account.account.status_changed.v1"
	// AccountCustomerCreatedV1 is dps.account.v1.CustomerCreated.
	AccountCustomerCreatedV1 Name = "account.customer.created.v1"
	// AccountCustomerKYCLevelChangedV1 is dps.account.v1.CustomerKycLevelChanged.
	AccountCustomerKYCLevelChangedV1 Name = "account.customer.kyc_level_changed.v1"
	// AccountCatalogPublishedV1 is dps.account.v1.CatalogPublished.
	AccountCatalogPublishedV1 Name = "account.catalog.published.v1"
	// AccountContactPointChangedV1 is dps.account.v1.ContactPointChanged.
	AccountContactPointChangedV1 Name = "account.contact_point.changed.v1"
)

var accountDomain = Domain{
	Name:  "account",
	Topic: AccountTopic,
	Events: []Name{
		AccountAccountOpenedV1,
		AccountAccountStatusChangedV1,
		AccountCustomerCreatedV1,
		AccountCustomerKYCLevelChangedV1,
		AccountCatalogPublishedV1,
		AccountContactPointChangedV1,
	},
}
