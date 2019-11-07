package com.biologis.solr.client.solrj.io.ops;


/**
 * Creates new fields.
 * <p>
 * Example:
 * addField(value=EUR, as=currency)
 * <p>
 * Example's output:
 * "currency" : "EUR"
 */

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.ops.StreamOperation;
import org.apache.solr.client.solrj.io.stream.expr.*;
import org.apache.solr.client.solrj.io.stream.expr.Explanation.ExpressionType;

import java.io.IOException;
import java.util.Locale;
import java.util.UUID;

public class AddField implements StreamOperation {
    private static final long serialVersionUID = 1;
    private UUID operationNodeId = UUID.randomUUID();

    private String as;
    private String value;

    public AddField(String as, String value) {
        this.as = as;       // the field will be named 'as'
        this.value = value; // the value will be attached (added)
    }

    public AddField(StreamExpression expression, StreamFactory factory) throws IOException {

        if (expression.getParameters().size() == 2) {
            StreamExpressionNamedParameter asParam = factory.getNamedOperand(expression, "as");
            this.as = ((StreamExpressionValue) asParam.getParameter()).getValue();

            StreamExpressionNamedParameter value = factory.getNamedOperand(expression, "value");
            this.value = ((StreamExpressionValue) value.getParameter()).getValue();
        } else {
            throw new IOException(String.format(Locale.ROOT,
                    "Invalid expression %s - unknown operands found", expression));
        }
    }

    @Override
    public void operate(Tuple tuple) {
        StringBuilder buf = new StringBuilder();

        if (value == null) {
            value = "null";
        }

        buf.append(value);
        tuple.put(as, buf.toString());
    }

    @Override
    public StreamExpressionParameter toExpression(StreamFactory factory) throws IOException {
        StreamExpression expression = new StreamExpression(factory.getFunctionName(this.getClass()));

        expression.addParameter(new StreamExpressionNamedParameter("value", value));
        expression.addParameter(new StreamExpressionNamedParameter("as", as));

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