-- Seeds the soak source tables. Same DDL/row-shape as etl-host's OracleSource test fixture
-- (etl-host/src/test/kotlin/etlhost/HostEndToEndOracleTest.kt), so the seeded groups match
-- application.properties' etl-host.cache.sql.wip / .equipment out of the box.
create table lot (id number(18), lot_id varchar2(40), qty number(18,3), site varchar2(8));
insert into lot select level, 'L' || level, level * 1.5,
  case when mod(level, 2) = 0 then 'F12' else 'F11' end
  from dual connect by level <= 500;
create table equipment (id number(18), tool_id varchar2(40), state varchar2(8));
insert into equipment select level, 'T' || level,
  case when mod(level, 2) = 0 then 'UP' else 'DOWN' end
  from dual connect by level <= 40;
commit;
select count(*) as lot_rows from lot;
select count(*) as equipment_rows from equipment;
exit;
