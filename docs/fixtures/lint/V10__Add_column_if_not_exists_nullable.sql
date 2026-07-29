-- Every ceremony writes the threshold NOT NULL.
-- The comment above must not make this look like a NOT NULL column, and the
-- column name is `threshold`, not `IF`. This file must lint clean.
ALTER TABLE dicom.reid_shares
  ADD COLUMN IF NOT EXISTS threshold smallint
    CHECK (threshold IS NULL OR threshold BETWEEN 1 AND 255);
