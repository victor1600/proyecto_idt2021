CREATE TABLE "s3_schemas"."airline_dimension"
( carrier character varying(256) encode lzo, airline_name character varying(256) encode lzo,
start_date date NOT NULL encode az64, end_date date encode az64, current_flag boolean NOT NULL, airline_key bigint NOT NULL encode az64);