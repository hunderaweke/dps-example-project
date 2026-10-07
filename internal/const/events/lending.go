package events

// LendingTopic carries the events of MILKII Microlending (dps_lending): consents, assessments, offers, loans, repayments.
// The names mirror dps-contracts proto/dps/lending/v1/events.proto.
const LendingTopic = "lending.events"

const (
	// LendingConsentGrantedV1 is dps.lending.v1.ConsentGranted.
	LendingConsentGrantedV1 Name = "lending.consent.granted.v1"
	// LendingConsentRevokedV1 is dps.lending.v1.ConsentRevoked.
	LendingConsentRevokedV1 Name = "lending.consent.revoked.v1"
	// LendingAssessmentCompletedV1 is dps.lending.v1.AssessmentCompleted.
	LendingAssessmentCompletedV1 Name = "lending.assessment.completed.v1"
	// LendingOfferPresentedV1 is dps.lending.v1.OfferPresented.
	LendingOfferPresentedV1 Name = "lending.offer.presented.v1"
	// LendingOfferAcceptedV1 is dps.lending.v1.OfferAccepted.
	LendingOfferAcceptedV1 Name = "lending.offer.accepted.v1"
	// LendingOfferExpiredV1 is dps.lending.v1.OfferExpired.
	LendingOfferExpiredV1 Name = "lending.offer.expired.v1"
	// LendingLoanDisbursedV1 is dps.lending.v1.LoanDisbursed.
	LendingLoanDisbursedV1 Name = "lending.loan.disbursed.v1"
	// LendingLoanDisbursementFailedV1 is dps.lending.v1.LoanDisbursementFailed.
	LendingLoanDisbursementFailedV1 Name = "lending.loan.disbursement_failed.v1"
	// LendingLoanOverdueV1 is dps.lending.v1.LoanOverdue.
	LendingLoanOverdueV1 Name = "lending.loan.overdue.v1"
	// LendingLoanClosedV1 is dps.lending.v1.LoanClosed.
	LendingLoanClosedV1 Name = "lending.loan.closed.v1"
	// LendingRepaymentReceivedV1 is dps.lending.v1.RepaymentReceived.
	LendingRepaymentReceivedV1 Name = "lending.repayment.received.v1"
)

var lendingDomain = Domain{
	Name:  "lending",
	Topic: LendingTopic,
	Events: []Name{
		LendingConsentGrantedV1,
		LendingConsentRevokedV1,
		LendingAssessmentCompletedV1,
		LendingOfferPresentedV1,
		LendingOfferAcceptedV1,
		LendingOfferExpiredV1,
		LendingLoanDisbursedV1,
		LendingLoanDisbursementFailedV1,
		LendingLoanOverdueV1,
		LendingLoanClosedV1,
		LendingRepaymentReceivedV1,
	},
}
