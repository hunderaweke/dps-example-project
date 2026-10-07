package events

// PaymentTopic carries the events of Payment Request (dps_payment): transfers, ETQR, request to pay, pre-authorizations, beneficiaries, merchants.
// The names mirror dps-contracts proto/dps/payment/v1/events.proto.
const PaymentTopic = "payment.events"

const (
	// PaymentPaymentInitiatedV1 is dps.payment.v1.PaymentInitiated.
	PaymentPaymentInitiatedV1 Name = "payment.payment.initiated.v1"
	// PaymentPaymentCompletedV1 is dps.payment.v1.PaymentCompleted.
	PaymentPaymentCompletedV1 Name = "payment.payment.completed.v1"
	// PaymentPaymentFailedV1 is dps.payment.v1.PaymentFailed.
	PaymentPaymentFailedV1 Name = "payment.payment.failed.v1"
	// PaymentPaymentOutcomeUnknownV1 is dps.payment.v1.PaymentOutcomeUnknown.
	PaymentPaymentOutcomeUnknownV1 Name = "payment.payment.outcome_unknown.v1"
	// PaymentPaymentReversedV1 is dps.payment.v1.PaymentReversed.
	PaymentPaymentReversedV1 Name = "payment.payment.reversed.v1"
	// PaymentBeneficiaryAddedV1 is dps.payment.v1.BeneficiaryAdded.
	PaymentBeneficiaryAddedV1 Name = "payment.beneficiary.added.v1"
	// PaymentBeneficiaryDeletedV1 is dps.payment.v1.BeneficiaryDeleted.
	PaymentBeneficiaryDeletedV1 Name = "payment.beneficiary.deleted.v1"
	// PaymentRequestToPayCreatedV1 is dps.payment.v1.RequestToPayCreated.
	PaymentRequestToPayCreatedV1 Name = "payment.request_to_pay.created.v1"
	// PaymentRequestToPayPaidV1 is dps.payment.v1.RequestToPayPaid.
	PaymentRequestToPayPaidV1 Name = "payment.request_to_pay.paid.v1"
	// PaymentRequestToPayDeclinedV1 is dps.payment.v1.RequestToPayDeclined.
	PaymentRequestToPayDeclinedV1 Name = "payment.request_to_pay.declined.v1"
	// PaymentRequestToPayExpiredV1 is dps.payment.v1.RequestToPayExpired.
	PaymentRequestToPayExpiredV1 Name = "payment.request_to_pay.expired.v1"
	// PaymentPreAuthorizationHeldV1 is dps.payment.v1.PreAuthorizationHeld.
	PaymentPreAuthorizationHeldV1 Name = "payment.pre_authorization.held.v1"
	// PaymentPreAuthorizationCapturedV1 is dps.payment.v1.PreAuthorizationCaptured.
	PaymentPreAuthorizationCapturedV1 Name = "payment.pre_authorization.captured.v1"
	// PaymentPreAuthorizationReleasedV1 is dps.payment.v1.PreAuthorizationReleased.
	PaymentPreAuthorizationReleasedV1 Name = "payment.pre_authorization.released.v1"
	// PaymentPreAuthorizationExpiredV1 is dps.payment.v1.PreAuthorizationExpired.
	PaymentPreAuthorizationExpiredV1 Name = "payment.pre_authorization.expired.v1"
	// PaymentQRCodeGeneratedV1 is dps.payment.v1.QrCodeGenerated.
	PaymentQRCodeGeneratedV1 Name = "payment.qr_code.generated.v1"
	// PaymentQRCodeUsedV1 is dps.payment.v1.QrCodeUsed.
	PaymentQRCodeUsedV1 Name = "payment.qr_code.used.v1"
	// PaymentQRCodeExpiredV1 is dps.payment.v1.QrCodeExpired.
	PaymentQRCodeExpiredV1 Name = "payment.qr_code.expired.v1"
	// PaymentMerchantOnboardedV1 is dps.payment.v1.MerchantOnboarded.
	PaymentMerchantOnboardedV1 Name = "payment.merchant.onboarded.v1"
	// PaymentMerchantStatusChangedV1 is dps.payment.v1.MerchantStatusChanged.
	PaymentMerchantStatusChangedV1 Name = "payment.merchant.status_changed.v1"
)

var paymentDomain = Domain{
	Name:  "payment",
	Topic: PaymentTopic,
	Events: []Name{
		PaymentPaymentInitiatedV1,
		PaymentPaymentCompletedV1,
		PaymentPaymentFailedV1,
		PaymentPaymentOutcomeUnknownV1,
		PaymentPaymentReversedV1,
		PaymentBeneficiaryAddedV1,
		PaymentBeneficiaryDeletedV1,
		PaymentRequestToPayCreatedV1,
		PaymentRequestToPayPaidV1,
		PaymentRequestToPayDeclinedV1,
		PaymentRequestToPayExpiredV1,
		PaymentPreAuthorizationHeldV1,
		PaymentPreAuthorizationCapturedV1,
		PaymentPreAuthorizationReleasedV1,
		PaymentPreAuthorizationExpiredV1,
		PaymentQRCodeGeneratedV1,
		PaymentQRCodeUsedV1,
		PaymentQRCodeExpiredV1,
		PaymentMerchantOnboardedV1,
		PaymentMerchantStatusChangedV1,
	},
}
