UNINSTALL PLUGIN mysqlite;
INSTALL PLUGIN mysqlite SONAME 'libmysqlite_engine.so';
CREATE FUNCTION sqlite_db RETURNS INT SONAME 'libmysqlite_engine.so';
