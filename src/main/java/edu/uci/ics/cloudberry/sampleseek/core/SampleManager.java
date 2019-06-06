package edu.uci.ics.cloudberry.sampleseek.core;

import edu.uci.ics.cloudberry.sampleseek.SampleSeekMain;

import java.lang.reflect.Array;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;

public class SampleManager {

    private final String separator = " | ";
    private final int outputSize = 10;

    private String baseTableName = null;
    private String[] baseTableColumnNames = null;
    private String sampleTableName = null;
    private String[] sampleTableColumnNames = null;
    private String[] sampleTableColumnTypes = null;
    private Class[] sampleTableColumnClasses = null;

    private int baseTableSize = 0;
    private int sampleTableSize = 10;


    private Map<String, Object> sample = new HashMap<>();

    public SampleManager() {

        this.baseTableName = SampleSeekMain.config.getSampleConfig().getBaseTableName();
        this.baseTableColumnNames = SampleSeekMain.config.getSampleConfig().getBaseTableColumnNames();
        this.sampleTableName = SampleSeekMain.config.getSampleConfig().getSampleTableName();
        this.sampleTableColumnNames = SampleSeekMain.config.getSampleConfig().getSampleTableColumnNames();
        this.sampleTableColumnTypes = SampleSeekMain.config.getSampleConfig().getSampleTableColumnTypes();
        this.sampleTableColumnClasses = SampleSeekMain.config.getSampleConfig().getSampleTalbeColumnClasses();

        this.baseTableSize = SampleSeekMain.config.getSampleConfig().getBaseTableSize();

        // sample size = sqrt(n) / epsilon^2
        this.sampleTableSize = (int) Math.round(Math.sqrt(baseTableSize) /
                        Math.pow(SampleSeekMain.config.getSeekConfig().getEpsilon(), 2)
        );

        // initialize the sample data structure
        for (int i = 0; i < this.sampleTableColumnNames.length; i ++) {
            String name = this.sampleTableColumnNames[i];
            this.sample.put(name, Array.newInstance(this.sampleTableColumnClasses[i], this.sampleTableSize));
        }
    }

    public boolean generateSample() {
        // Generate uniform sample with replacement from table
        String sql = "CREATE TABLE " + this.sampleTableName + " AS " +
                "WITH t AS (SELECT ";

        for (int j = 0; j < this.baseTableColumnNames.length; j ++) {
            if (j > 0) {
                sql += ", ";
            }
            sql += this.baseTableColumnNames[j] + " AS " + this.sampleTableColumnNames[j];
        }

        sql +=
        ", row_number() OVER () AS rn FROM " + this.baseTableName + ")\n" +
                "SELECT * FROM (\n" +
                "    SELECT trunc(random() * (SELECT max(rn) FROM t))::int + 1 AS rn\n" +
                "    FROM   generate_series(1, ?) g\n" +
                "    ) r\n" +
                "JOIN   t USING (rn)";

        System.out.println("Generating sample with size: " + this.sampleTableSize + " ... ...");
        System.out.println("SQL: " + sql);

        try {
            PreparedStatement statement = SampleSeekMain.conn.prepareStatement(sql);
            statement.setInt(1, this.sampleTableSize);
            statement.executeUpdate();
            statement.close();

            return true;
        } catch (SQLException e) {
            System.err.println(e.getMessage());
            return false;
        }
    }

    public boolean isSampeExist() {
        boolean exist = false;
        // Generate uniform sample with replacement from table
        String sql = "SELECT 1 FROM " + this.sampleTableName + " LIMIT 1";

        try {
            PreparedStatement statement = SampleSeekMain.conn.prepareStatement(sql);
            ResultSet rs = statement.executeQuery();
            if (rs.next()) {
                exist = true;
            }
            statement.close();
        } catch (SQLException e) {
            //System.err.println(e.getMessage());
        }

        return exist;
    }

    public boolean loadSample() {
        String sql = "SELECT ";
        for (int j = 0; j < this.sampleTableColumnNames.length; j ++) {
            if (j > 0) {
                sql += ",";
            }
            sql += this.sampleTableColumnNames[j];
        }
        sql += " FROM " + this.sampleTableName;

        System.out.println("SQL: " + sql);

        try {
            PreparedStatement statement = SampleSeekMain.conn.prepareStatement(sql);
            ResultSet rs = statement.executeQuery();
            int rsId = 0;
            while (rs.next()) {
                for (int j = 0; j < this.sampleTableColumnNames.length; j ++) {
                    String columnName = this.sampleTableColumnNames[j];
                    Object columnStore = this.sample.get(columnName);
                    Array.set(columnStore, rsId, this.sampleTableColumnClasses[j].cast(rs.getObject(columnName)));
                }
                rsId ++;
            }

            if (rsId < this.sampleTableSize) {
                System.err.println("\n[Warning] The real size [" + rsId +
                        "] of sample table in DB is smaller than the size [" + this.sampleTableSize +
                        "] calculated using epsilon. We will use the real size.");
                this.sampleTableSize = rsId;
            }
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }

        return true;
    }

    public void printSample() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < Math.min(outputSize, this.sampleTableSize); i ++) {
            sb.setLength(0);
            sb.append(i);
            for (int j = 0; j < this.sampleTableColumnNames.length; j ++) {
                sb.append(separator);
                sb.append(Array.get(this.sample.get(this.sampleTableColumnNames[j]), i));
            }
            System.out.println(sb.toString());
        }
        if (outputSize < this.sampleTableSize) {
            System.out.println("... ...");
            System.out.println("(total " + this.sampleTableSize + " lines)");
        }
    }

    public String getSampleTableName() {
        return sampleTableName;
    }

    public String[] getSampleTableColumnNames() {
        return sampleTableColumnNames;
    }

    public String[] getSampleTableColumnTypes() {
        return sampleTableColumnTypes;
    }

    public Class[] getSampleTableColumnClasses() {
        return sampleTableColumnClasses;
    }

    public int getSampleTableSize() {
        return sampleTableSize;
    }

    public int getBaseTableSize() {
        return baseTableSize;
    }

    public Map<String, Object> getSample() {
        return sample;
    }

    public String getSampleTableColumnType(String columnName) {
        for (int i = 0; i < sampleTableColumnNames.length; i ++ ) {
            if (sampleTableColumnNames[i].equalsIgnoreCase(columnName)) {
                return sampleTableColumnTypes[i];
            }
        }
        return null;
    }
}
