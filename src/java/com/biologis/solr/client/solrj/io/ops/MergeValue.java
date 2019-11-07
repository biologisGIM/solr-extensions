package com.biologis.solr.client.solrj.io.ops;

/**
 * Merge the existing field value
 * with the custom value provided
 * by the user, without any changes
 * in the field title
 * <p>
 * Example:
 * mergeValue(field=price, delim="_", mergeValue=EUR
 * <p>
 * Output:
 * "price": "9.99_EUR"
 */

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.ops.StreamOperation;
import org.apache.solr.client.solrj.io.stream.expr.*;
import org.apache.solr.client.solrj.io.stream.expr.Explanation.ExpressionType;

import java.io.IOException;
import java.util.Locale;
import java.util.UUID;

public class MergeValue implements StreamOperation {

    private static final long serialVersionUID = 1;
    private UUID operationNodeId = UUID.randomUUID();

    private String field;
    private String mergeValue;
    private String delim;

    public MergeValue(String field, String mergeValue, String delim) {
        this.field = field;
        this.mergeValue = mergeValue;
        this.delim = delim;
    }

    public MergeValue(StreamExpression expression, StreamFactory factory) throws IOException {

        if (expression.getParameters().size() == 3) {
            StreamExpressionNamedParameter fieldParam = factory.getNamedOperand(expression, "field");
            this.field = ((StreamExpressionValue) fieldParam.getParameter()).getValue();

            StreamExpressionNamedParameter mergeValueParam = factory.getNamedOperand(expression, "mergeValue");
            this.mergeValue = ((StreamExpressionValue) mergeValueParam.getParameter()).getValue();

            StreamExpressionNamedParameter delimParam = factory.getNamedOperand(expression, "delim");
            this.delim = ((StreamExpressionValue) delimParam.getParameter()).getValue();
        } else {
            throw new IOException(String.format(Locale.ROOT,
                    "Bad expression %s - no such an expression found!", expression));
        }
    }

    @Override
    public void operate(Tuple tuple) {
        StringBuilder sb = new StringBuilder();
        Object val = tuple.get(field);

        sb.append(val);
        sb.append(delim);
        sb.append(mergeValue);
        tuple.put(field, sb.toString());
    }

    @Override
    public StreamExpressionParameter toExpression(StreamFactory factory) throws IOException {
        StreamExpression expression = new StreamExpression(factory.getFunctionName(this.getClass()));

        StringBuilder sb = new StringBuilder();

        expression.addParameter(new StreamExpressionNamedParameter("filed", sb.toString()));
        expression.addParameter(new StreamExpressionNamedParameter("mergeValue", mergeValue));
        expression.addParameter(new StreamExpressionNamedParameter("delim", delim));

        return expression;
    }

    @Override
    public Explanation toExplanation(StreamFactory factory) throws IOException {
        return new Explanation(operationNodeId.toString())
                .withExpressionType(ExpressionType.OPERATION)
                .withFunctionName(factory.getFunctionName(getClass()))
                .withImplementingClass(getClass().getName())
                .withExpression(toExpression(factory).toString());
    }
}
