CREATE TABLE IF NOT EXISTS stock.company_profile (
    stock_code        varchar(10) NOT NULL,
    company_name      text,
    emiten_type       varchar(10),
    sector            text,
    sub_sector        text,
    industry          text,
    sub_industry      text,
    listing_date_text text,
    website           text,
    address           text,
    phone             text,
    fax               text,
    email             text,
    npwp              text,
    status            text,
    raw               jsonb NOT NULL,
    created_at        timestamp DEFAULT now(),
    updated_at        timestamp DEFAULT now(),
    PRIMARY KEY (stock_code)
);

CREATE OR REPLACE FUNCTION stock.upsert_company_profiles(rows jsonb)
RETURNS integer
LANGUAGE plpgsql
AS $$
DECLARE
    rec jsonb;
    inserted_count integer := 0;
BEGIN
    IF rows IS NULL OR jsonb_typeof(rows) <> 'array' THEN
        RAISE EXCEPTION 'rows must be a jsonb array';
    END IF;

    FOR rec IN SELECT * FROM jsonb_array_elements(rows)
    LOOP
        INSERT INTO stock.company_profile (
            stock_code,
            company_name,
            emiten_type,
            sector,
            sub_sector,
            industry,
            sub_industry,
            listing_date_text,
            website,
            address,
            phone,
            fax,
            email,
            npwp,
            status,
            raw,
            updated_at
        ) VALUES (
            NULLIF(rec->>'KodeEmiten', ''),
            NULLIF(rec->>'NamaEmiten', ''),
            NULLIF(rec->>'EmitenType', ''),
            NULLIF(rec->>'Sektor', ''),
            NULLIF(rec->>'SubSektor', ''),
            NULLIF(rec->>'Industri', ''),
            NULLIF(rec->>'SubIndustri', ''),
            NULLIF(rec->>'TglPencatatan', ''),
            NULLIF(rec->>'Website', ''),
            NULLIF(rec->>'Alamat', ''),
            NULLIF(rec->>'Telepon', ''),
            NULLIF(rec->>'Fax', ''),
            NULLIF(rec->>'Email', ''),
            NULLIF(rec->>'NPWP', ''),
            NULLIF(rec->>'Status', ''),
            rec,
            now()
        )
        ON CONFLICT (stock_code)
        DO UPDATE SET
            company_name = EXCLUDED.company_name,
            emiten_type = EXCLUDED.emiten_type,
            sector = EXCLUDED.sector,
            sub_sector = EXCLUDED.sub_sector,
            industry = EXCLUDED.industry,
            sub_industry = EXCLUDED.sub_industry,
            listing_date_text = EXCLUDED.listing_date_text,
            website = EXCLUDED.website,
            address = EXCLUDED.address,
            phone = EXCLUDED.phone,
            fax = EXCLUDED.fax,
            email = EXCLUDED.email,
            npwp = EXCLUDED.npwp,
            status = EXCLUDED.status,
            raw = EXCLUDED.raw,
            updated_at = now();

        inserted_count := inserted_count + 1;
    END LOOP;

    RETURN inserted_count;
END;
$$;
