package com.yugabyte.cdcsdk.testing.util;

public class IOT implements TestTable {

    public final String tableName;
    public final int numTablets;

    private final String createStatement = "CREATE TABLE %s (id SERIAL PRIMARY KEY,"
            + "host_id int,"
            + "date timestamp NOT NULL,"
            + "cpu double PRECISION,"
            + "tempc int) ";

    private final String createYBStmt = createStatement + "SPLIT INTO %d TABLETS;";

    private final String insertStmt = "INSERT INTO %s(date,host_id,cpu,tempc) SELECT date, host_id,"
            + "random() * 100 AS cpu, random() * 70 AS tempc "
            + "FROM generate_series('2022-07-01'::date, '2022-07-01'::Date + INTERVAL '1 minute',"
            + " INTERVAL '10 seconds') AS date, generate_series(1,10) AS host_id;";

    private final String dropStatement = "DROP TABLE %s;";

    public IOT(String tableName, int numTablets) {
        this.tableName = tableName;
        this.numTablets = numTablets;
    }

    public IOT(String tableName) {
        this(tableName, 1);
    }

    @Override
    public String getCreateTableYBStmt() {
        return String.format(this.createYBStmt, this.tableName, this.numTablets);
    }

    @Override
    public String getCreateTablePgStmt() {
        return String.format(this.createStatement, this.tableName);
    }

    @Override
    public String insertStmt() {
        return String.format(this.insertStmt, this.tableName);
    }

    @Override
    public String dropTable() {
        return String.format(this.dropStatement, this.tableName);
    }
}