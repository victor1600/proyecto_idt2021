CREATE TABLE "s3_schemas"."monthly_dimension"(month       integer encode az64,
                                              quarter     integer encode az64,
                                              year        integer encode az64,
                                              monthly_key bigint NOT NULL encode az64);