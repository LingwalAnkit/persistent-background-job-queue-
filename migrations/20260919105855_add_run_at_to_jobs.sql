-- Add migration script here
ALTER TABLE jobs ADD COLUMN run_at TIMESTAMPTZ NOT NULL DEFAULT now();
