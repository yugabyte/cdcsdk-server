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
    private static final String DROP_TABLE_FORMAT = "DROP TABLE IF EXISTS %s;";

    // Format string with customizable table name and the values for the columns
    private static final String INSERT_FORMAT = "INSERT INTO %s VALUES (%d, '%s', '%s', %f);";

    private static final String DELETE_FORMAT = "DELETE FROM %s WHERE id = %d;";

    private static final String UPDATE_FORMAT = "UPDATE %s SET days_worked = %d WHERE id = %d";

    public static final String DATABASE_SERVER_NAME = "dbserver1";

    public static final String ALL_TYPES_TABLE_NAME = "all_types";

    public static final String CREATE_ALL_TYPES_TABLE = "CREATE TABLE " + ALL_TYPES_TABLE_NAME
            + " (id INT PRIMARY KEY, "
            + "bigintcol bigint, bitcol bit(5), varbitcol varbit(5), booleanval boolean, byteaval bytea, "
            + "ch char(5), vchar varchar(25), cidrval cidr, dt date, dp double precision, inetval inet, "
            + "intervalval interval, jsonval json, jsonbval jsonb, mc macaddr, mc8 macaddr8, mn money, "
            + "nm numeric, rl real, si smallint, i4r int4range, i8r int8range, nr numrange, tsr tsrange, "
            + "tstzr tstzrange, dr daterange, txt text, tm time, tmtz timetz, ts timestamp, "
            + "tstz timestamptz, uuidval uuid)";

    public static final String INSERT_ALL_TYPES_FORMAT = "INSERT INTO " + ALL_TYPES_TABLE_NAME
            + " (id, bigintcol, bitcol, varbitcol, booleanval, byteaval, ch, vchar, cidrval, dt, dp, inetval, "
            + "intervalval, jsonval, jsonbval, mc, mc8, mn, nm, rl, si, i4r, i8r, nr, tsr, tstzr, dr, txt, tm, tmtz, ts, tstz, uuidval) VALUES "
            + "(%d, 123456, '11011', '10101', FALSE, E'\\\\001', 'five5', 'sample_text', '10.1.0.0/16', '2022-02-24', 12.345, '127.0.0.1', "
            + "'2020-03-10 00:00:00'::timestamp-'2020-02-10 00:00:00'::timestamp, '{\"a\":\"b\"}', '{\"a\":\"b\"}', '2C:54:91:88:C9:E3', '22:00:5c:03:55:08:01:02', '$100.500', "
            + "12.345, 32.145, 12, '(1, 10)', '(100, 200)', '(10.45, 21.32)', '(1970-01-01 00:00:00, 2000-01-01 12:00:00)', "
            + "'(2017-07-04 12:30:30 UTC, 2021-07-04 12:30:30+05:30)', '(2019-10-07, 2021-10-07)', 'text to verify behaviour', '12:47:32', '12:00:00+05:30', "
            + "'2021-11-25 12:00:00', '2021-11-25 12:00:00+05:30', 'ffffffff-ffff-ffff-ffff-ffffffffffff');";

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
    public static String getCreateTableYBStmt(String tableName) {
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

    public static String getDeleteStmt(String tableName, int id) {
        return String.format(DELETE_FORMAT, tableName, id);
    }

    public static String getUpdateStmt(String tableName, int id, int days_worked) {
        return String.format(UPDATE_FORMAT, tableName, days_worked, id);
    }
}
