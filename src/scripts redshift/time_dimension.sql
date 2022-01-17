CREATE TABLE "s3_schemas"."time_dimension"
( time_key integer encode az64, time_24h character varying(256) encode lzo, hour integer encode az64,
minute integer encode az64, second integer encode az64, hour_12 integer encode az64, time_ampm character varying(256) encode lzo,
meridian_indicator character varying(256) encode lzo, period character varying(256) encode lzo);