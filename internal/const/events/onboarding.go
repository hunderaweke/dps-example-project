package events

// OnboardingTopic carries the events of Onboarding and KYC (dps_onboarding): applications, consents, documents, Fayda KYC, review.
// The names mirror dps-contracts proto/dps/onboarding/v1/events.proto.
const OnboardingTopic = "onboarding.events"

const (
	// OnboardingApplicationStartedV1 is dps.onboarding.v1.ApplicationStarted.
	OnboardingApplicationStartedV1 Name = "onboarding.application.started.v1"
	// OnboardingApplicationConsentedV1 is dps.onboarding.v1.ApplicationConsented.
	OnboardingApplicationConsentedV1 Name = "onboarding.application.consented.v1"
	// OnboardingDocumentUploadedV1 is dps.onboarding.v1.DocumentUploaded.
	OnboardingDocumentUploadedV1 Name = "onboarding.document.uploaded.v1"
	// OnboardingKYCPassedV1 is dps.onboarding.v1.KycPassed.
	OnboardingKYCPassedV1 Name = "onboarding.kyc.passed.v1"
	// OnboardingKYCFlaggedV1 is dps.onboarding.v1.KycFlagged.
	OnboardingKYCFlaggedV1 Name = "onboarding.kyc.flagged.v1"
	// OnboardingApplicationReturnedV1 is dps.onboarding.v1.ApplicationReturned.
	OnboardingApplicationReturnedV1 Name = "onboarding.application.returned.v1"
	// OnboardingApplicationCompletedV1 is dps.onboarding.v1.ApplicationCompleted.
	OnboardingApplicationCompletedV1 Name = "onboarding.application.completed.v1"
	// OnboardingApplicationRejectedV1 is dps.onboarding.v1.ApplicationRejected.
	OnboardingApplicationRejectedV1 Name = "onboarding.application.rejected.v1"
	// OnboardingApplicationExpiredV1 is dps.onboarding.v1.ApplicationExpired.
	OnboardingApplicationExpiredV1 Name = "onboarding.application.expired.v1"
	// OnboardingApplicationBlockedV1 is dps.onboarding.v1.ApplicationBlocked.
	OnboardingApplicationBlockedV1 Name = "onboarding.application.blocked.v1"
)

var onboardingDomain = Domain{
	Name:  "onboarding",
	Topic: OnboardingTopic,
	Events: []Name{
		OnboardingApplicationStartedV1,
		OnboardingApplicationConsentedV1,
		OnboardingDocumentUploadedV1,
		OnboardingKYCPassedV1,
		OnboardingKYCFlaggedV1,
		OnboardingApplicationReturnedV1,
		OnboardingApplicationCompletedV1,
		OnboardingApplicationRejectedV1,
		OnboardingApplicationExpiredV1,
		OnboardingApplicationBlockedV1,
	},
}
