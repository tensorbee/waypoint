-- The first statement is exempted by name, with a mandatory justification.
-- The second is not, and still raises E001.
-- waypoint:lint-ignore E001 reason="table is empty until the backfill job runs"
ALTER TABLE dicom.reid_shares ADD COLUMN IF NOT EXISTS quorum smallint NOT NULL;

ALTER TABLE dicom.reid_shares ADD COLUMN IF NOT EXISTS ceiling smallint NOT NULL;
