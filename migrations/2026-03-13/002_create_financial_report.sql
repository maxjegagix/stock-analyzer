CREATE TABLE IF NOT EXISTS stock.financial_report (
    stock_code    varchar(10) NOT NULL,
    report_year   integer NOT NULL,
    report_period text NOT NULL,
    report_type   text NOT NULL,
    file_modified timestamp,
    company_name  text,
    raw           jsonb NOT NULL,
    created_at    timestamp DEFAULT now(),
    updated_at    timestamp DEFAULT now(),
    PRIMARY KEY (stock_code, report_year, report_period, report_type)
);

CREATE TABLE IF NOT EXISTS stock.financial_report_attachment (
    stock_code    varchar(10) NOT NULL,
    report_year   integer NOT NULL,
    report_period text NOT NULL,
    report_type   text NOT NULL,
    file_id       text NOT NULL,
    file_name     text,
    file_path     text,
    file_size     bigint,
    file_type     text,
    file_modified timestamp,
    raw           jsonb NOT NULL,
    created_at    timestamp DEFAULT now(),
    updated_at    timestamp DEFAULT now(),
    PRIMARY KEY (stock_code, report_year, report_period, report_type, file_id)
);

CREATE OR REPLACE FUNCTION stock.upsert_financial_reports_payload(payload jsonb)
RETURNS integer
LANGUAGE plpgsql
AS $$
DECLARE
    rec jsonb;
    att jsonb;
    inserted_count integer := 0;
    v_stock_code text;
    v_report_year integer;
    v_report_period text;
    v_report_type text;
    v_file_modified timestamp;
    v_company_name text;
    v_file_id text;
BEGIN
    IF payload IS NULL OR jsonb_typeof(payload) <> 'object' THEN
        RAISE EXCEPTION 'payload must be a jsonb object';
    END IF;

    IF payload->'Results' IS NULL OR jsonb_typeof(payload->'Results') <> 'array' THEN
        RAISE EXCEPTION 'payload.Results must be a jsonb array';
    END IF;

    FOR rec IN SELECT * FROM jsonb_array_elements(payload->'Results')
    LOOP
        v_stock_code := NULLIF(rec->>'KodeEmiten', '');
    v_report_year := COALESCE(
        NULLIF(rec->>'Report_Year', '')::integer,
        NULLIF(payload#>>'{Search,Year}', '')::integer
    );
    v_report_period := COALESCE(
        NULLIF(rec->>'Report_Period', ''),
        NULLIF(payload#>>'{Search,Periode}', ''),
        'unknown'
    );
    v_report_type := COALESCE(
        NULLIF(rec->>'Report_Type', ''),
        NULLIF(payload#>>'{Search,ReportType}', ''),
        'unknown'
    );
        v_file_modified := NULLIF(rec->>'File_Modified', '')::timestamp;
        v_company_name := NULLIF(rec->>'NamaEmiten', '');

        INSERT INTO stock.financial_report (
            stock_code,
            report_year,
            report_period,
            report_type,
            file_modified,
            company_name,
            raw,
            updated_at
        ) VALUES (
            v_stock_code,
            v_report_year,
            v_report_period,
            v_report_type,
            v_file_modified,
            v_company_name,
            rec,
            now()
        )
        ON CONFLICT (stock_code, report_year, report_period, report_type)
        DO UPDATE SET
            file_modified = EXCLUDED.file_modified,
            company_name = EXCLUDED.company_name,
            raw = EXCLUDED.raw,
            updated_at = now();

        inserted_count := inserted_count + 1;

        IF rec->'Attachments' IS NOT NULL AND jsonb_typeof(rec->'Attachments') = 'array' THEN
            FOR att IN SELECT * FROM jsonb_array_elements(rec->'Attachments')
            LOOP
                v_file_id := NULLIF(att->>'File_ID', '');

                INSERT INTO stock.financial_report_attachment (
                    stock_code,
                    report_year,
                    report_period,
                    report_type,
                    file_id,
                    file_name,
                    file_path,
                    file_size,
                    file_type,
                    file_modified,
                    raw,
                    updated_at
                ) VALUES (
                    COALESCE(NULLIF(att->>'Emiten_Code', ''), v_stock_code),
                    COALESCE(NULLIF(att->>'Report_Year', '')::integer, v_report_year),
                    COALESCE(NULLIF(att->>'Report_Period', ''), v_report_period),
                    COALESCE(NULLIF(att->>'Report_Type', ''), v_report_type),
                    v_file_id,
                    NULLIF(att->>'File_Name', ''),
                    NULLIF(att->>'File_Path', ''),
                    NULLIF(att->>'File_Size', '')::bigint,
                    NULLIF(att->>'File_Type', ''),
                    NULLIF(att->>'File_Modified', '')::timestamp,
                    att,
                    now()
                )
                ON CONFLICT (stock_code, report_year, report_period, report_type, file_id)
                DO UPDATE SET
                    file_name = EXCLUDED.file_name,
                    file_path = EXCLUDED.file_path,
                    file_size = EXCLUDED.file_size,
                    file_type = EXCLUDED.file_type,
                    file_modified = EXCLUDED.file_modified,
                    raw = EXCLUDED.raw,
                    updated_at = now();
            END LOOP;
        END IF;
    END LOOP;

    RETURN inserted_count;
END;
$$;
