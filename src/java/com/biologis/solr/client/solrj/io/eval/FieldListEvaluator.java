package com.biologis.solr.client.solrj.io.eval;

import org.apache.solr.client.solrj.io.eval.ManyValueWorker;
import org.apache.solr.client.solrj.io.eval.RecursiveObjectEvaluator;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionNamedParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

import java.io.IOException;
import java.util.List;


public class FieldListEvaluator extends RecursiveObjectEvaluator implements ManyValueWorker {
    protected static final long serialVersionUID = 1L;
    private String delim = ",";

    public FieldListEvaluator(StreamExpression expression, StreamFactory factory) throws IOException {
        super (expression, factory);

        List<StreamExpressionNamedParameter> namedParams = factory.getNamedOperands(expression);

        for (StreamExpressionNamedParameter namedParam : namedParams){
            if (namedParam.getName().equals("delim")){
                this.delim = namedParam.getParameter().toString().trim();
            } else {
                throw new IOException("Unexpected named parameter:"+namedParam.getName());
            }
        }
    }

    @Override
    public Object doWork(Object values[]) throws IOException {

        StringBuilder sb = new StringBuilder();

        for (Object ob : values) {
            if (sb.length() > 0) {
                sb.append(delim);
            }
            String str = ob.toString();
            if (str.startsWith("\"") && str.endsWith("\"")) {
                str = str.substring(1, str.length() -1);
            } else if (str == null || str.isEmpty() || str.equals("")) {        //TODO: must be edited to append the "und" string if isEmpty
                str = "und";
            }
            sb.append(str.toString());
        }

        return sb.toString();
    }
}