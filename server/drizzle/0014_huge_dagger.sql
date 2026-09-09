-- Vibeploy production keeps the legacy uptime tables for rollback safety.
-- The application no longer reads them, and leaving them in place is harmless.
-- This migration is intentionally non-destructive so an older backend can be
-- restored without losing uptime history.
SELECT 1;
