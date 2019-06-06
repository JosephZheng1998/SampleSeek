package edu.uci.ics.cloudberry.sampleseek.core;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import edu.uci.ics.cloudberry.sampleseek.SampleSeekMain;
import edu.uci.ics.cloudberry.sampleseek.model.Operator;
import edu.uci.ics.cloudberry.sampleseek.model.Query;

import java.lang.reflect.Array;
import java.sql.*;

public class QueryExecutor {

    private final int outputSize = 10;

    private long threshold;
    private String baseTableName;

    private SampleManager sampleManager;
    private SeekManager seekManager;

    public QueryExecutor(SampleManager sampleManager, SeekManager seekManager) {
        this.sampleManager = sampleManager;
        this.seekManager = seekManager;
        this.threshold = Math.round(1.0 / Math.pow(SampleSeekMain.config.getSeekConfig().getEpsilon(), 2));
        this.baseTableName = SampleSeekMain.config.getSampleConfig().getBaseTableName();
    }

    public JsonNode executeQuery (Query query) {
        System.out.println("Executing query: \n" + query);
        long start = System.currentTimeMillis();
        ObjectNode result = JsonNodeFactory.instance.objectNode();

        int numberOfRecords = 0;

        // no group by queries
        if (query.getGroupBy() == null) {
            System.out.println("1 --> Scan sample ... ...");
            // scan the whole sample table and return records meeting filter predicates
            ArrayNode resultArray = result.putArray("result");
            for (int i = 0; i < sampleManager.getSampleTableSize(); i ++) {
                if (tupleHit(query, i)) {
                    // project values for SELECT attributes
                    ArrayNode resultTuple = JsonNodeFactory.instance.arrayNode();
                    for (int j = 0; j < query.getSelect().length; j ++) {
                        String attribute = query.getSelect()[j];
                        Object columnStore = sampleManager.getSample().get(attribute);
                        switch (sampleManager.getSampleTableColumnType(attribute).toLowerCase()) {
                            case "int":
                            case "integer":
                            case "number":
                                resultTuple.add((int)Array.get(columnStore, i));
                                break;
                            case "bigint":
                                resultTuple.add((long)Array.get(columnStore, i));
                                break;
                            case "double":
                                resultTuple.add((double)Array.get(columnStore, i));
                                break;
                            default:
                                resultTuple.add(String.valueOf(Array.get(columnStore, i)));
                                break;
                        }
                    }
                    resultArray.add(resultTuple);
                    numberOfRecords ++;
                }
            }
            result.put("length", resultArray.size());
            result.put("sampleTableSize", sampleManager.getSampleTableSize());
            result.put("baseTableSize", sampleManager.getBaseTableSize());
        }
        // group by queries
        else {
            System.out.println("group by queries are not supported yet. return ...");
            // TODO - Similar to non-groupy queries, scan the sample first to compute results,
            //        then check whether number of records meet the threshold requirement...
            return result;
        }

        // number of records does NOT meet the threshold requirement
        if (numberOfRecords < this.threshold) {
            System.out.println("Oops! # of result records (" + numberOfRecords + ") is < threshold (" + this.threshold + ")");
            result.removeAll();
            System.out.println("2 --> Online sampling from indexes and base table ... ...");
            result = (ObjectNode) executeQueryWithIndexes(query);
        }
        else {
            System.out.println("Good! # of result records (" + numberOfRecords + ") is > threshold (" + this.threshold + ")");
        }

        long end = System.currentTimeMillis();
        System.out.println("Executing query DONE!  Time: " + String.format("%.3f", (end - start)/1000.0) + " seconds");

        // output sample result
        outputSampleResult(result);

        return result;
    }

    private void outputSampleResult(ObjectNode result) {
        ArrayNode resultArray = (ArrayNode) result.get("result");
        System.out.println("=== Query result ===");
        System.out.println("{");
        System.out.println(" \"sampleTableSize\": " + result.get("sampleTableSize") + ", ");
        System.out.println(" \"baseTableSize\": " + result.get("baseTableSize") + ", ");
        System.out.println(" \"length\": " + resultArray.size() + ", ");
        System.out.println(" \"result\": [");
        for (int i = 0; i < Math.min(outputSize, resultArray.size()); i ++) {
            System.out.println(resultArray.get(i));
        }
        System.out.println("... ...");
        System.out.println("(total " + resultArray.size() + " lines)");
    }

    private JsonNode executeQueryWithIndexes(Query query) {

        JsonNode result = null;

        // get count of records meeting filters predicates
        int count = countOfQueryResults(query);

        // if count can not NOT meet the threshold requirement, just query DB for original query
        if (count < this.threshold) {
            System.out.println("2(1) --> Send original query to the base table  ... ...");
            result = queryDBForOriginalQuery(query);
        }
        // else get all ids from indexes first, then do online sampling
        else {
            // TODO - get all ids from SeekManager, then online sample 1/epsilon^2 ids,
            //        then send random I/O query to DB to get the exact records / group-by results
        }

        return result;
    }

    private int countOfQueryResults(Query query) {
        // TODO - Currently ignore the case when a small query's result size >= threshold 1/epsilon^2,
        //        to implement this, we need to get the count from SeekManager.
        return 0;
    }

    private JsonNode queryDBForOriginalQuery(Query query) {
        ObjectNode result = JsonNodeFactory.instance.objectNode();

        String sql = generateSQLForOriginalQuery(query);

        try {
            PreparedStatement statement = SampleSeekMain.conn.prepareStatement(sql);
            ResultSet rs = statement.executeQuery();
            ResultSetMetaData meta = rs.getMetaData();
            int columnCount = meta.getColumnCount();
            ArrayNode resultArray = result.putArray("result");
            while (rs.next()) {
                ArrayNode resultTuple = JsonNodeFactory.instance.arrayNode();
                for (int j = 1; j <= columnCount; j ++) {
                    int columnType = meta.getColumnType(j);
                    switch (columnType) {
                        case Types.INTEGER:
                            resultTuple.add(rs.getInt(j));
                            break;
                        case Types.BIGINT:
                            resultTuple.add(rs.getLong(j));
                            break;
                        case Types.DOUBLE:
                            resultTuple.add(rs.getDouble(j));
                            break;
                        case Types.TIMESTAMP_WITH_TIMEZONE:
                        case Types.TIMESTAMP:
                            resultTuple.add(rs.getTimestamp(j).toString());
                            break;
                        default:
                            resultTuple.add(rs.getString(j));
                    }

                }
                resultArray.add(resultTuple);
            }

            result.put("length", resultArray.size());
            result.put("sampleTableSize", sampleManager.getBaseTableSize());
            result.put("baseTableSize", sampleManager.getBaseTableSize());
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }
        return result;
    }

    private String generateSQLForOriginalQuery(Query query) {
        String sql = null;

        // no group by queries
        if (query.getGroupBy() == null) {
            sql = "SELECT ";
            for (int j = 0; j < query.getSelect().length; j ++) {
                if (j > 0) {
                    sql += ",";
                }
                sql += query.getSelect()[j];
            }
            sql += " FROM " + this.baseTableName + " ";

            sql += generateWhereClauseForQuery(query);
        }
        // group by queries
        else {
            System.out.println("group by queries are not supported yet. return ...");
        }
        return sql;
    }

    private String generateWhereClauseForQuery(Query query) {
        String whereClause = "WHERE 1=1 ";

        // for each filter
        for (int j = 0; j < query.getFilters().length; j ++) {
            whereClause += "AND ";

            String attribute = query.getFilters()[j].getAttribute();
            Operator operator = query.getFilters()[j].getOperator();
            String[] operands = query.getFilters()[j].getOperands();
            String columnType = sampleManager.getSampleTableColumnType(attribute);
            switch (columnType.toLowerCase()) {
                case "int":
                case "integer":
                case "number":
                case "bigint":
                case "double":
                    break;
                case "timestamp":
                default:
                    for (int k = 0; k < operands.length; k ++) {
                        operands[k] = "'" + operands[k] + "'";
                    }
            }
            switch (operator) {
                case IN:
                    whereClause += attribute + " >= " + operands[0] + " AND " + attribute + " <= " + operands[1] + " ";
                    break;
                case LT:
                    whereClause += attribute + " <= " + operands[0] + " ";
                    break;
                case GT:
                    whereClause += attribute + " >= " + operands[0] + " ";
                    break;
                case EQUAL:
                    whereClause += attribute + " = " + operands[0] + " ";
                    break;
            }
        }

        return whereClause;
    }

    private boolean tupleHit(Query query, int i) {
        boolean hit = true;

        // for each filter
        for (int j = 0; j < query.getFilters().length; j ++) {
            String attribute = query.getFilters()[j].getAttribute();
            Operator operator = query.getFilters()[j].getOperator();
            String[] operands = query.getFilters()[j].getOperands();

            Object columnStore = sampleManager.getSample().get(attribute);
            String columnType = sampleManager.getSampleTableColumnType(attribute);

            switch (columnType.toLowerCase()) {
                case "int":
                case "integer":
                case "number":
                    switch (operator) {
                        case IN:
                            hit = new ExpressionEvaluator<Integer>()
                                    .evaluateDoubleOperand((Integer) Array.get(columnStore, i),
                                            operator, Integer.valueOf(operands[0]), Integer.valueOf(operands[1]));
                            break;
                        default:
                            hit = new ExpressionEvaluator<Integer>()
                                    .evaluateSingleOperand((Integer) Array.get(columnStore, i),
                                            operator, Integer.valueOf(operands[0]));
                    }
                    break;
                case "bigint":
                    switch (operator) {
                        case IN:
                            hit = new ExpressionEvaluator<Long>()
                                    .evaluateDoubleOperand((Long) Array.get(columnStore, i),
                                            operator, Long.valueOf(operands[0]), Long.valueOf(operands[1]));
                            break;
                        default:
                            hit = new ExpressionEvaluator<Long>()
                                    .evaluateSingleOperand((Long) Array.get(columnStore, i),
                                            operator, Long.valueOf(operands[0]));
                    }
                    break;
                case "double":
                    switch (operator) {
                        case IN:
                            hit = new ExpressionEvaluator<Double>()
                                    .evaluateDoubleOperand((Double) Array.get(columnStore, i),
                                            operator, Double.valueOf(operands[0]), Double.valueOf(operands[1]));
                            break;
                        default:
                            hit = new ExpressionEvaluator<Double>()
                                    .evaluateSingleOperand((Double) Array.get(columnStore, i),
                                            operator, Double.valueOf(operands[0]));
                    }
                    break;
                case "timestamp":
                    Timestamp valueTime = (Timestamp) Array.get(columnStore, i);
                    Timestamp leftTime = Timestamp.valueOf(operands[0]);
                    switch (operator) {
                        case IN:
                            Timestamp rightTime = Timestamp.valueOf(operands[1]);
                            if (valueTime.after(rightTime) || valueTime.before(leftTime)) {
                                hit = false;
                            }
                            break;
                        case LT:
                            if (valueTime.after(leftTime)) {
                                hit = false;
                            }
                            break;
                        case GT:
                            if (valueTime.before(leftTime)) {
                                hit = false;
                            }
                            break;
                        case EQUAL:
                            if (!valueTime.equals(leftTime)) {
                                hit = false;
                            }
                            break;
                    }
                    break;
                default:
                    String valueString = (String) Array.get(columnStore, i);
                    String leftString = operands[0];
                    switch (operator) {
                        case IN:
                            String rightString = operands[1];
                            if (valueString.compareTo(leftString) < 0 || valueString.compareTo(rightString) > 0) {
                                hit = false;
                            }
                            break;
                        case LT:
                            if (valueString.compareTo(leftString) > 0) {
                                hit = false;
                            }
                            break;
                        case GT:
                            if (valueString.compareTo(leftString) < 0) {
                                hit = false;
                            }
                            break;
                        case EQUAL:
                            if (!valueString.equals(leftString)) {
                                hit = false;
                            }
                            break;
                    }
                    break;
            }
        }
        return hit;
    }

    public class ExpressionEvaluator<T extends Number & Comparable<? super T>> {
        public boolean evaluateSingleOperand(T value, Operator operator, T operand) {
            switch (operator) {
                case LT:
                    if (value.compareTo(operand) <= 0) {
                        return true;
                    }
                case GT:
                    if (value.compareTo(operand) >= 0) {
                        return true;
                    }
                case EQUAL:
                    if (value.compareTo(operand) == 0) {
                        return true;
                    }
            }
            return false;
        }

        public boolean evaluateDoubleOperand(T value, Operator operator, T left, T right) {
            switch (operator) {
                case IN:
                    if (value.compareTo(left) >= 0 && value.compareTo(right) <= 0) {
                        return true;
                    }
            }
            return false;
        }
    }
}
