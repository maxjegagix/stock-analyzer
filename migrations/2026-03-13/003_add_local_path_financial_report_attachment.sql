ALTER TABLE IF EXISTS stock.financial_report_attachment
    ADD COLUMN IF NOT EXISTS local_path text,
    ADD COLUMN IF NOT EXISTS downloaded_at timestamp;
