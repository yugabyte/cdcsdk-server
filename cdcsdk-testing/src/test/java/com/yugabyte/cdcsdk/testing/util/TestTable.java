package com.yugabyte.cdcsdk.testing.util;

public interface TestTable {

    /**
     * Wrapper function around {@link #getCreateTableYBStmt(String, int)} to get a
     * create table statement with 1 tablet
     *
     * @param tableName name of the table
     * @return the CREATE TABLE query for a single tablet
     */
    public String getCreateTableYBStmt();

    /**
     * Get a create table statement for table to be created in Postgres database
     *
     * @param tableName name of the table
     * @return the CREATE TABLE query
     */
    public String getCreateTablePgStmt();

    /**
     * Get an insert table statement for values to be inserted into the provided
     * table
     *
     * @return The insert statement to insert into the table.
     */
    public String insertStmt();

    /**
     * Get a drop table statement for table to be dropped. Works for both YugabyteDB
     * and Postgres database
     *
     * @param tableName name of the table
     * @return the DROP TABLE query
     */
    public String dropTable();
}
