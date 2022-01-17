CREATE TABLE "s3_schemas"."plane_dimension"
( n_number character varying(256) encode lzo, serial_number character varying(256) encode lzo,
registrant_name character varying(256) encode lzo, aircraft_mfr_model character varying(256) encode lzo,
year_mfr integer encode az64, registrant_city character varying(256) encode lzo, registrant_state character varying(256) encode lzo,
registrant_street character varying(256) encode lzo, eng_mfr_model_code integer encode az64, eng_mfr_name character varying(256) encode lzo,
eng_model_name character varying(256) encode lzo, eng_horse_power integer encode az64, start_date date NOT NULL encode az64,
end_date date encode az64, current_flag boolean NOT NULL, plane_key bigint NOT NULL encode az64);