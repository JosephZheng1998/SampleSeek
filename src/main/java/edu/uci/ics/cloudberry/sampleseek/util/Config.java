package edu.uci.ics.cloudberry.sampleseek.util;

import java.sql.Timestamp;
import java.util.Map;

public class Config {

    public class DBConfig {
        private String url;
        private String username;
        private String password;

        public DBConfig() {
        }

        public void setUrl(String url) {
            this.url = url;
        }

        public void setUsername(String username) {
            this.username = username;
        }

        public void setPassword(String password) {
            this.password = password;
        }

        public String getUrl() {
            return url;
        }

        public String getUsername() {
            return username;
        }

        public String getPassword() {
            return password;
        }
    }

    public class SampleConfig {
        private String baseTableName;
        private String[] baseTableColumnNames;
        private int baseTableSize;
        private String sampleTableName;
        private String[] sampleTableColumnNames;
        private String[] sampleTableColumnTypes;
        private Class[] sampleTalbeColumnClasses;

        public SampleConfig() {
        }

        public void setBaseTableName(String baseTableName) {
            this.baseTableName = baseTableName;
        }

        public void setBaseTableColumnNames(String[] baseTableColumnNames) {
            this.baseTableColumnNames = baseTableColumnNames;
        }

        public void setBaseTableSize(int baseTableSize) {
            this.baseTableSize = baseTableSize;
        }

        public void setSampleTableName(String sampleTableName) {
            this.sampleTableName = sampleTableName;
        }

        public void setSampleTableColumnNames(String[] sampleTableColumnNames) {
            this.sampleTableColumnNames = sampleTableColumnNames;
        }

        public void setSampleTableColumnTypes(String[] sampleTableColumnTypes) {
            this.sampleTableColumnTypes = sampleTableColumnTypes;
            this.sampleTalbeColumnClasses = new Class[this.sampleTableColumnTypes.length];
            for (int i = 0; i < this.sampleTableColumnTypes.length; i ++) {
                Class colClass = null;
                switch (this.sampleTableColumnTypes[i].toLowerCase()) {
                    case "int":
                    case "integer":
                    case "number":
                        colClass = Integer.class;
                        break;
                    case "bigint":
                        colClass = Long.class;
                        break;
                    case "timestamp":
                        colClass = Timestamp.class;
                        break;
                    case "double":
                        colClass = Double.class;
                        break;
                    default:
                        colClass = String.class;
                        break;
                }
                this.sampleTalbeColumnClasses[i] = colClass;
            }
        }

        public void setSampleTalbeColumnClasses(Class[] sampleTalbeColumnClasses) {
            this.sampleTalbeColumnClasses = sampleTalbeColumnClasses;
        }

        public String getBaseTableName() {
            return baseTableName;
        }

        public String[] getBaseTableColumnNames() {
            return baseTableColumnNames;
        }

        public int getBaseTableSize() {
            return baseTableSize;
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

        public Class[] getSampleTalbeColumnClasses() {
            return sampleTalbeColumnClasses;
        }
    }

    public class SeekConfig {
        private String pkey;
        private String[] measures;
        private String[] dimensions;
        private DimensionType[] dimensionTypes;
        private double epsilon;

        public SeekConfig() {
        }

        public void setPkey(String pkey) {
            this.pkey = pkey;
        }

        public void setMeasures(String[] measures) {
            this.measures = measures;
        }

        public void setDimensions(String[] dimensions) {
            this.dimensions = dimensions;
        }

        public void setDimensionTypes(DimensionType[] dimensionTypes) {
            this.dimensionTypes = dimensionTypes;
        }

        public void setEpsilon(double epsilon) {
            this.epsilon = epsilon;
        }

        public String getPkey() {
            return pkey;
        }

        public String[] getMeasures() {
            return measures;
        }

        public String[] getDimensions() {
            return dimensions;
        }

        public DimensionType[] getDimensionTypes() {
            return dimensionTypes;
        }

        public double getEpsilon() {
            return epsilon;
        }
    }

    private Map<String, String> serverConfig;
    private DBConfig dbConfig;
    private SampleConfig sampleConfig;
    private SeekConfig seekConfig;

    public Config() {
    }

    public void setServerConfig(Map<String, String> serverConfig) {
        this.serverConfig = serverConfig;
    }

    public void setDbConfig(DBConfig dbConfig) {
        this.dbConfig = dbConfig;
    }

    public void setSampleConfig(SampleConfig sampleConfig) {
        this.sampleConfig = sampleConfig;
    }

    public void setSeekConfig(SeekConfig seekConfig) {
        this.seekConfig = seekConfig;
    }

    public Map<String, String> getServerConfig() {
        return serverConfig;
    }

    public DBConfig getDbConfig() {
        return dbConfig;
    }

    public SampleConfig getSampleConfig() {
        return sampleConfig;
    }

    public SeekConfig getSeekConfig() {
        return seekConfig;
    }
}


