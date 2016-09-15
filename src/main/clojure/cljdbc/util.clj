(ns cljdbc.util
  (:require
    [cljdbc :as j])
  (:import
    [java.sql
     ResultSet
     PreparedStatement]))

(def jdbc-string->driver-map
  "Map used to map jdbc url to driver map."
  {"derby"      "org.apache.derby.jdbc.ClientDataSource"
   "firebirt"   "org.firebirdsql.pool.FBSimpleDataSource"
   "h2"         "org.h2.jdbcx.JdbcDataSource"
   "hsqldb"     "org.hsqldb.jdbc.JDBCDataSource"
   "db2"        "com.ibm.db2.jcc.DB2SimpleDataSource"
   "infomix"    "com.informix.jdbcx.IfxDataSource"
   "mssql"      "com.microsoft.sqlserver.jdbc.SQLServerDataSource"
   "mysql"      "com.mysql.jdbc.jdbc2.optional.MysqlDataSource" ;; org.mariadb.jdbc.MySQLDataSource
   "oracle"     "oracle.jdbc.pool.OracleDataSource"
   "orient"     "com.orientechnologies.orient.jdbc.OrientDataSource"
   "postgresql" "com.impossibl.postgres.jdbc.PGDataSource"      ;; org.postgresql.ds.PGSimpleDataSource
   "sqlite"     "org.sqlite.SQLiteDataSource"
   "sybase"     "com.sybase.jdbc4.jdbc.SybDataSource"})
