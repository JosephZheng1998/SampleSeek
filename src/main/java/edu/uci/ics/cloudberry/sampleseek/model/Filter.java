package edu.uci.ics.cloudberry.sampleseek.model;

public class Filter {
    private String attribute;
    private Operator operator;
    private String[] operands;

    public Filter() {
    }

    public void setAttribute(String attribute) {
        this.attribute = attribute;
    }

    public void setOperator(Operator operator) {
        this.operator = operator;
    }

    public void setOperands(String[] operands) {
        this.operands = operands;
    }

    public String getAttribute() {
        return attribute;
    }

    public Operator getOperator() {
        return operator;
    }

    public String[] getOperands() {
        return operands;
    }
}
