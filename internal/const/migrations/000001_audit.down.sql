DROP TRIGGER IF EXISTS audit_records_append_only ON audit_records;
DROP FUNCTION IF EXISTS audit_records_refuse_change();
DROP TABLE IF EXISTS audit_records;
