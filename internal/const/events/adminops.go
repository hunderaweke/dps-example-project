package events

// AdminOpsTopic carries the events of Admin Ops (dps_adminops): maker-checker changes, cases and disputes, reports.
// The names mirror dps-contracts proto/dps/adminops/v1/events.proto.
const AdminOpsTopic = "adminops.events"

const (
	// AdminopsChangeSubmittedV1 is dps.adminops.v1.ChangeSubmitted.
	AdminopsChangeSubmittedV1 Name = "adminops.change.submitted.v1"
	// AdminopsChangeApprovedV1 is dps.adminops.v1.ChangeApproved.
	AdminopsChangeApprovedV1 Name = "adminops.change.approved.v1"
	// AdminopsChangeRejectedV1 is dps.adminops.v1.ChangeRejected.
	AdminopsChangeRejectedV1 Name = "adminops.change.rejected.v1"
	// AdminopsChangeAppliedV1 is dps.adminops.v1.ChangeApplied.
	AdminopsChangeAppliedV1 Name = "adminops.change.applied.v1"
	// AdminopsChangeApplyFailedV1 is dps.adminops.v1.ChangeApplyFailed.
	AdminopsChangeApplyFailedV1 Name = "adminops.change.apply_failed.v1"
	// AdminopsChangeExpiredV1 is dps.adminops.v1.ChangeExpired.
	AdminopsChangeExpiredV1 Name = "adminops.change.expired.v1"
	// AdminopsCaseOpenedV1 is dps.adminops.v1.CaseOpened.
	AdminopsCaseOpenedV1 Name = "adminops.case.opened.v1"
	// AdminopsCaseAssignedV1 is dps.adminops.v1.CaseAssigned.
	AdminopsCaseAssignedV1 Name = "adminops.case.assigned.v1"
	// AdminopsCaseEscalatedV1 is dps.adminops.v1.CaseEscalated.
	AdminopsCaseEscalatedV1 Name = "adminops.case.escalated.v1"
	// AdminopsCaseClosedV1 is dps.adminops.v1.CaseClosed.
	AdminopsCaseClosedV1 Name = "adminops.case.closed.v1"
	// AdminopsReportRunCompletedV1 is dps.adminops.v1.ReportRunCompleted.
	AdminopsReportRunCompletedV1 Name = "adminops.report_run.completed.v1"
)

var adminopsDomain = Domain{
	Name:  "adminops",
	Topic: AdminOpsTopic,
	Events: []Name{
		AdminopsChangeSubmittedV1,
		AdminopsChangeApprovedV1,
		AdminopsChangeRejectedV1,
		AdminopsChangeAppliedV1,
		AdminopsChangeApplyFailedV1,
		AdminopsChangeExpiredV1,
		AdminopsCaseOpenedV1,
		AdminopsCaseAssignedV1,
		AdminopsCaseEscalatedV1,
		AdminopsCaseClosedV1,
		AdminopsReportRunCompletedV1,
	},
}
