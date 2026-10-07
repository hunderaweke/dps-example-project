package events

// AuthTopic carries the events of Auth and PDP (dps_auth): sessions, devices, PIN, OTP, policy decisions, staff roles.
// The names mirror dps-contracts proto/dps/auth/v1/events.proto.
const AuthTopic = "auth.events"

const (
	// AuthSessionStartedV1 is dps.auth.v1.SessionStarted.
	AuthSessionStartedV1 Name = "auth.session.started.v1"
	// AuthDeviceRegisteredV1 is dps.auth.v1.DeviceRegistered.
	AuthDeviceRegisteredV1 Name = "auth.device.registered.v1"
	// AuthPaymentDecidedV1 is dps.auth.v1.PaymentDecided.
	AuthPaymentDecidedV1 Name = "auth.payment.decided.v1"
	// AuthOTPSentV1 is dps.auth.v1.OtpSent.
	AuthOTPSentV1 Name = "auth.otp.sent.v1"
	// AuthOTPVerifiedV1 is dps.auth.v1.OtpVerified.
	AuthOTPVerifiedV1 Name = "auth.otp.verified.v1"
	// AuthOTPFailedV1 is dps.auth.v1.OtpFailed.
	AuthOTPFailedV1 Name = "auth.otp.failed.v1"
	// AuthSignInFailedV1 is dps.auth.v1.SignInFailed.
	AuthSignInFailedV1 Name = "auth.sign_in.failed.v1"
	// AuthSessionRevokedV1 is dps.auth.v1.SessionRevoked.
	AuthSessionRevokedV1 Name = "auth.session.revoked.v1"
	// AuthPINChangedV1 is dps.auth.v1.PinChanged.
	AuthPINChangedV1 Name = "auth.pin.changed.v1"
	// AuthCredentialLockedV1 is dps.auth.v1.CredentialLocked.
	AuthCredentialLockedV1 Name = "auth.credential.locked.v1"
	// AuthTokenFamilyRevokedV1 is dps.auth.v1.TokenFamilyRevoked.
	AuthTokenFamilyRevokedV1 Name = "auth.token_family.revoked.v1"
	// AuthPaymentReleasedV1 is dps.auth.v1.PaymentReleased.
	AuthPaymentReleasedV1 Name = "auth.payment.released.v1"
	// AuthPolicyBundleActivatedV1 is dps.auth.v1.PolicyBundleActivated.
	AuthPolicyBundleActivatedV1 Name = "auth.policy_bundle.activated.v1"
	// AuthStaffRoleGrantedV1 is dps.auth.v1.StaffRoleGranted.
	AuthStaffRoleGrantedV1 Name = "auth.staff_role.granted.v1"
	// AuthStaffRoleRevokedV1 is dps.auth.v1.StaffRoleRevoked.
	AuthStaffRoleRevokedV1 Name = "auth.staff_role.revoked.v1"
)

var authDomain = Domain{
	Name:  "auth",
	Topic: AuthTopic,
	Events: []Name{
		AuthSessionStartedV1,
		AuthDeviceRegisteredV1,
		AuthPaymentDecidedV1,
		AuthOTPSentV1,
		AuthOTPVerifiedV1,
		AuthOTPFailedV1,
		AuthSignInFailedV1,
		AuthSessionRevokedV1,
		AuthPINChangedV1,
		AuthCredentialLockedV1,
		AuthTokenFamilyRevokedV1,
		AuthPaymentReleasedV1,
		AuthPolicyBundleActivatedV1,
		AuthStaffRoleGrantedV1,
		AuthStaffRoleRevokedV1,
	},
}
