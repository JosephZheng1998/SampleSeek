package edu.uci.ics.cloudberry.sampleseek.core;

import edu.uci.ics.cloudberry.sampleseek.SampleSeekMain;
import edu.uci.ics.cloudberry.sampleseek.util.DimensionType;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class SeekManager {

    private String pkey;
    private String[] measures;
    private String[] dimensions;
    private DimensionType[] dimensionTypes;
    private String baseTableName;

    /**
     * map of [dimesionName -> indexInfo]
     * e.g. "create_at" -> indexInfo
     *     indexInfo is a map of [key -> value]
     *     e.g. "indexName" -> "idx_tweets_create_at_id"
     *          "inDB"      -> true
     */
    private Map<String, Map<String, Object>> indexes = new HashMap<>();

    public SeekManager() {
        this.pkey = SampleSeekMain.config.getSeekConfig().getPkey();
        this.measures = SampleSeekMain.config.getSeekConfig().getMeasures();
        this.dimensions = SampleSeekMain.config.getSeekConfig().getDimensions();
        this.dimensionTypes = SampleSeekMain.config.getSeekConfig().getDimensionTypes();
        this.baseTableName = SampleSeekMain.config.getSampleConfig().getBaseTableName();
    }

    public int buildIndexes() {
        int successCount = 0;

        // for each dimension d, we need to build a composite index on (d, pkey)
        for (int i = 0; i < this.dimensions.length; i ++) {
            // generate sql
            String sql = "CREATE INDEX IF NOT EXISTS ";
            String dimensionName = this.dimensions[i];
            String indexName = "idx_";
            if (dimensionName.equalsIgnoreCase(this.pkey)) {
                indexName += this.baseTableName + "_" + this.pkey;
            } else {
                indexName += this.baseTableName + "_" + dimensionName + "_" + this.pkey;
            }
            sql += indexName + " ON " + this.baseTableName + " USING ";
            switch (this.dimensionTypes[i]) {
                case CATEGORICAL:
                    sql += "GIN ";
                    break;
                case NUMERICAL:
                default:
                    sql += "BTREE ";
            }
            sql += "(";
            if (dimensionName.equalsIgnoreCase(this.pkey)) {
                sql += this.pkey + ")";
            }
            else {
                sql += dimensionName + ", " + this.pkey + ")";
            }

            System.out.println("Building index [" + indexName + "] on dimension: " + dimensionName);
            System.out.println("SQL: " + sql);

            // send sql to database
            try {
                PreparedStatement statement = SampleSeekMain.conn.prepareStatement(sql);
                statement.executeUpdate();
                statement.close();

                successCount ++;

                // write index info into map of indexes
                Map<String, Object> indexInfo = new HashMap<>();
                indexInfo.put("indexName", indexName);
                indexInfo.put("inDB", true);
                indexes.put(dimensionName, indexInfo);
            } catch (SQLException e) {
                System.err.println("[Warning] Building index [" + indexName + "] failed, exception:");
                System.err.println(e.getMessage());
            }
        }

        return successCount;
    }
}
