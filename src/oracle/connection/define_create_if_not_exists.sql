create or replace procedure create_if_not_exists(input_sql varchar2)
as
begin
    execute immediate input_sql;
    exception
    when others then
    if sqlcode = -955 then
        NULL;
    else
        raise;
    end if;
end;
