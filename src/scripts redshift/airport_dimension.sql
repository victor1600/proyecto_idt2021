CREATE TABLE "s3_schemas"."airport_dimension"
( code character varying(256) encode lzo, airport_name character varying(256) encode lzo,
city character varying(256) encode lzo, state_code character varying(256) encode lzo,
state_name character varying(256) encode lzo, wac integer encode az64, country character varying(256) encode lzo,
start_date date NOT NULL encode az64, end_date date encode az64, current_flag boolean NOT NULL, airport_key bigint NOT NULL encode az64);