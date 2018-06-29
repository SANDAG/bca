@echo off


set script_path=
set db_server=
set db_name=
rem db_path and log_path values must be enclosed in double quotes
set db_path=
set log_path=

echo Creating %db_name% on %db_server% at %db_path%
echo Log file at %log_path%
sqlcmd -E -C -b -S %db_server% -i %script_path%create_bca_db.sql -v db_name=%db_name% db_path=%db_path% log_path=%log_path% || goto :EOF

echo Creating db_meta schema and objects
sqlcmd -E -C -b -S %db_server% -d %db_name% -i %script_path%db_meta.sql || goto :EOF