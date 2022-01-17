CREATE TABLE "s3_schemas"."date_dimension"
( date_key integer encode az64, full_date_description character varying(256) encode lzo,
date character varying(256) encode lzo, day_of_week integer encode az64, calendar_month integer encode az64,
calenda_quarter integer encode az64, calendar_year integer encode az64, holiday_indicator character varying(256) encode lzo,
weekday_indicator character varying(256) encode lzo);