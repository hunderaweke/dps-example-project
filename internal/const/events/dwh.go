package events

// DWHTopic carries the events of Data Warehouse (dps_dwh): loads and backfills.
// The names mirror dps-contracts proto/dps/dwh/v1/events.proto.
const DWHTopic = "dwh.events"

const (
	// DWHBackfillCompletedV1 is dps.dwh.v1.BackfillCompleted.
	DWHBackfillCompletedV1 Name = "dwh.backfill.completed.v1"
	// DWHLoadRunFailedV1 is dps.dwh.v1.LoadRunFailed.
	DWHLoadRunFailedV1 Name = "dwh.load_run.failed.v1"
)

var dwhDomain = Domain{
	Name:  "dwh",
	Topic: DWHTopic,
	Events: []Name{
		DWHBackfillCompletedV1,
		DWHLoadRunFailedV1,
	},
}
