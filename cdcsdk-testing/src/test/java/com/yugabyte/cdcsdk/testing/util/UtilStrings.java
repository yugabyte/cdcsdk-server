package com.yugabyte.cdcsdk.testing.util;

/**
 * Helper class for providing various strings for DMLs and DDLs
 * 
 * @author Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */
public class UtilStrings {
  // Format string with customizable number of tablets and table name
  private static final String CREATE_TABLE_YB_FORMAT = "CREATE TABLE IF NOT EXISTS %s (id int primary key, first_name varchar(30), last_name varchar(50), days_worked double precision) SPLIT INTO %d TABLETS;";

  // Format string with customizable table name for Postgres
  private static final String CREATE_TABLE_PG_FORMAT = "CREATE TABLE IF NOT EXISTS %s (id int primary key, first_name varchar(30), last_name varchar(50), days_worked double precision);";

  // Format string with customizable name which can be used in both YugabyteDB and
  // Postgres
  private static final String DROP_TABLE_FORMAT = "DROP TABLE %s;";

  // Format string with customizable table name and the values for the columns
  private static final String INSERT_FORMAT = "INSERT INTO test_table VALUES (%d, '%s', '%s', %f);";

  /**
   * Get a create table statement for table to be created in YugabyteDB database
   * 
   * @param tableName       name of the table
   * @param numberOfTablets number of tablets the table should be split into
   * @return the CREATE TABLE query
   */
  public static String getCreateTableYBStmt(String tableName, int numberOfTablets) {
    return String.format(CREATE_TABLE_YB_FORMAT, tableName, numberOfTablets);
  }

  /**
   * Wrapper function around {@link #getCreateTableYBStmt(String, int)} to get a
   * create table statement with 1 tablet
   * 
   * @param tableName name of the table
   * @return the CREATE TABLE query for a single tablet
   */
  public static String getCreateTableYbStmt(String tableName) {
    return getCreateTableYBStmt(tableName, 1);
  }

  /**
   * Get a create table statement for table to be created in Postgres database
   * 
   * @param tableName name of the table
   * @return the CREATE TABLE query
   */
  public static String getCreateTablePgStmt(String tableName) {
    return String.format(CREATE_TABLE_PG_FORMAT, tableName);
  }

  /**
   * Get a drop table statement for table to be dropped. Works for both YugabyteDB
   * and Postgres database
   * 
   * @param tableName name of the table
   * @return the DROP TABLE query
   */
  public static String getDropTableStmt(String tableName) {
    return String.format(DROP_TABLE_FORMAT, tableName);
  }

  /**
   * Get an insert table statement for values to be inserted into the provided
   * table
   * 
   * @param tableName  name of the table
   * @param id         value of the column 'id'
   * @param firstName  value of the column 'first_name'
   * @param lastName   value of the column 'last_name'
   * @param daysWorked value of the column 'days_worked'
   * @return the INSERT query
   */
  public static String getInsertStmt(String tableName, int id, String firstName, String lastName, double daysWorked) {
    return String.format(INSERT_FORMAT, tableName, id, firstName, lastName, daysWorked);
  }
}
