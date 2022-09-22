package io.cdap.plugin.jdbc;
import java.util.LinkedHashMap;

public class DatabaseURLParser {
  // TODO: Implement Parsing Logic
  private static final String MYSQL_JDBC_URL_PREFIX = "jdbc:mysql";
  private static final String ORACLE_JDBC_URL_PREFIX = "jdbc:oracle";
  private static final String H2_JDBC_URL_PREFIX = "jdbc:h2";
  private static final String POSTGRESQL_JDBC_URL_PREFIX = "jdbc:postgresql";
  private static final String MARIADB_JDBC_URL_PREFIX = "jdbc:mariadb";
  private static final String SQLSERVER_JDBC_URL_PREFIX = "jdbc:sqlserver";
  private static final String DB2_JDBC_URL_PREFIX = "jdbc:db2";
  private static final String AS400_JDBC_URL_PREFIX = "jdbc:as400";

  public static String getFQN(String url) {
    return url;
  }
}
