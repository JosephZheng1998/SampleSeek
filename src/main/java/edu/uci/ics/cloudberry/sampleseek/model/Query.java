package edu.uci.ics.cloudberry.sampleseek.model;

public class Query {

    private String[] groupBy;
    private AggFunction aggFunction;
    private String aggAttribute;
    private String[] select;
    private Filter[] filters;

    public Query() {
    }

    public void setGroupBy(String[] groupBy) {
        this.groupBy = groupBy;
    }

    public void setAggFunction(AggFunction aggFunction) {
        this.aggFunction = aggFunction;
    }

    public void setAggAttribute(String aggAttribute) {
        this.aggAttribute = aggAttribute;
    }

    public void setSelect(String[] select) {
        this.select = select;
    }

    public void setFilters(Filter[] filters) {
        this.filters = filters;
    }

    public String[] getGroupBy() {
        return groupBy;
    }

    public AggFunction getAggFunction() {
        return aggFunction;
    }

    public String getAggAttribute() {
        return aggAttribute;
    }

    public String[] getSelect() {
        return select;
    }

    public Filter[] getFilters() {
        return filters;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("***** Query *****\n");
        if (groupBy != null) {
            sb.append("groupBy: [" + String.join(",", groupBy) + "]\n");
        }
        if (aggFunction != null) {
            sb.append("aggFunction: " + aggFunction + "\n");
        }
        if (aggAttribute != null) {
            sb.append("aggAttribute: " + aggAttribute + "\n");
        }
        if (select != null) {
            sb.append("select: [" + String.join(", ", select) + "]\n");
        }
        sb.append("filters: \n");
        for (Filter filter: filters) {
            sb.append("  [" + filter.getAttribute() + " ");
            sb.append(filter.getOperator() + " ");
            sb.append("\"" + String.join("\" AND \"", filter.getOperands()) + "\"]\n");
        }
        sb.append("***** ----- *****\n");
        return sb.toString();
    }
}

