CREATE TABLE "s3_schemas"."monthly_flight_fact"(airline_key        bigint encode az64,
                                                freight            double precision,
                                                passengers         double precision,
                                                mail               double precision,
                                                origin_airport_key bigint encode az64,
                                                dest_airport_key   bigint encode az64,
                                                monthly_key        bigint encode az64);