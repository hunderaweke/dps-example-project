package events

// LedgerTopic carries the events of Ledger (dps_ledger): operations, reconciliation, merchant settlement.
// The names mirror dps-contracts proto/dps/ledger/v1/events.proto.
const LedgerTopic = "ledger.events"

const (
	// LedgerOperationStartedV1 is dps.ledger.v1.OperationStarted.
	LedgerOperationStartedV1 Name = "ledger.operation.started.v1"
	// LedgerOperationCompletedV1 is dps.ledger.v1.OperationCompleted.
	LedgerOperationCompletedV1 Name = "ledger.operation.completed.v1"
	// LedgerOperationAbortedV1 is dps.ledger.v1.OperationAborted.
	LedgerOperationAbortedV1 Name = "ledger.operation.aborted.v1"
	// LedgerOperationOrphanedV1 is dps.ledger.v1.OperationOrphaned.
	LedgerOperationOrphanedV1 Name = "ledger.operation.orphaned.v1"
	// LedgerOperationResolvedV1 is dps.ledger.v1.OperationResolved.
	LedgerOperationResolvedV1 Name = "ledger.operation.resolved.v1"
	// LedgerOperationCapturedV1 is dps.ledger.v1.OperationCaptured.
	LedgerOperationCapturedV1 Name = "ledger.operation.captured.v1"
	// LedgerReconRunCompletedV1 is dps.ledger.v1.ReconRunCompleted.
	LedgerReconRunCompletedV1 Name = "ledger.recon_run.completed.v1"
	// LedgerReconExceptionRaisedV1 is dps.ledger.v1.ReconExceptionRaised.
	LedgerReconExceptionRaisedV1 Name = "ledger.recon_exception.raised.v1"
	// LedgerSettlementBatchPostedV1 is dps.ledger.v1.SettlementBatchPosted.
	LedgerSettlementBatchPostedV1 Name = "ledger.settlement_batch.posted.v1"
	// LedgerSettlementBatchFailedV1 is dps.ledger.v1.SettlementBatchFailed.
	LedgerSettlementBatchFailedV1 Name = "ledger.settlement_batch.failed.v1"
)

var ledgerDomain = Domain{
	Name:  "ledger",
	Topic: LedgerTopic,
	Events: []Name{
		LedgerOperationStartedV1,
		LedgerOperationCompletedV1,
		LedgerOperationAbortedV1,
		LedgerOperationOrphanedV1,
		LedgerOperationResolvedV1,
		LedgerOperationCapturedV1,
		LedgerReconRunCompletedV1,
		LedgerReconExceptionRaisedV1,
		LedgerSettlementBatchPostedV1,
		LedgerSettlementBatchFailedV1,
	},
}
