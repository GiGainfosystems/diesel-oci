declare
begin
    create_if_not_exists('CREATE TABLE "__DIESEL_SCHEMA_MIGRATIONS" (
        "VERSION" VARCHAR2(50) PRIMARY KEY NOT NULL,
        "RUN_ON" TIMESTAMP with time zone DEFAULT sysdate not null
    )');
end;
