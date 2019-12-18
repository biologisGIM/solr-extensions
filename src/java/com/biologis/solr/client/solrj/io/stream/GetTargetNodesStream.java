package com.biologis.solr.client.solrj.io.stream;

import com.biologis.solr.client.solrj.io.util.UniqueWorker;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.FieldComparator;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.stream.*;
import org.apache.solr.client.solrj.io.stream.expr.*;

import java.io.IOException;
import java.util.*;

public class GetTargetNodesStream extends CachedTupleStream implements Expressible {

    private TupleStream stream;
    private StreamComparator comparator;
    private UniqueWorker worker;
    private HashMap<String, String> options;


    public GetTargetNodesStream(StreamExpression expression, StreamFactory factory) throws IOException {
        List<StreamExpression> streamExpressions = factory.getExpressionOperandsRepresentingTypes(expression, new Class[]{Expressible.class, TupleStream.class});

        if (expression.getParameters().size() != streamExpressions.size()) {
            throw new IOException(String.format(Locale.ROOT, "Invalid expression %s - unknown operands found", expression));

        } else if (streamExpressions.size() != 1) {
            throw new IOException(String.format(Locale.ROOT, "Invalid expression %s - expecting one stream but found %d (must be TupleStream types)", expression, streamExpressions.size()));

        } else {
            this.init(factory.constructStream(streamExpressions.get(0)), factory);

        }
    }

    public void init(TupleStream stream, StreamFactory factory) throws IOException{
        this.stream = stream;
        this.comparator = factory.constructComparator("its_endpoint_id asc", FieldComparator.class);
        this.options = this.getSelectParameters();
        this.factory = factory;
        this.worker = new UniqueWorker("node", this.comparator);
    }

    private HashMap<String, String> getSelectParameters(){
        HashMap<String, String> options = new HashMap();

        options.put("its_endpoint_id", "its_endpoint_id");
        options.put("ss_endpoint_name", "ss_endpoint_name");
        options.put("ss_endpoint_type", "ss_endpoint_type");
        options.put("ss_search_api_id_container", "ss_search_api_id");
        options.put("node", "node");
        options.put("ancestors", "ancestors");

        return options;
    }

    @Override
    public void setStreamContext(StreamContext streamContext) {
        this.stream.setStreamContext(streamContext);
        this.streamContext = streamContext;
    }

    @Override
    public List<TupleStream> children() {
        return this.stream.children();
    }

    @Override
    protected void transform() throws IOException {
        this.stream.open();

        worker.readStream(stream);
        worker.sort();
    }

    @Override
    public void close() throws IOException {
        this.stream.close();
    }

    @Override
    protected Tuple readNormal() throws IOException {
        Tuple original = this.worker.read();

        if(original.EOF){
            return original;
        }

        Tuple result = new Tuple();

        for(Object field : original.fields.keySet()){
            if(this.options.containsKey(field)){
                result.put(this.options.get(field), original.get(field));
            }
        }

        return result;
    }

    @Override
    public StreamComparator getStreamSort() {
        return comparator;
    }

    @Override
    public StreamExpressionParameter toExpression(StreamFactory streamFactory) throws IOException {
        StreamExpression expression = new StreamExpression(factory.getFunctionName(this.getClass()));

        if (stream instanceof Expressible) {
            expression.addParameter(((Expressible) stream).toExpression(factory));
        } else {
            throw new IOException("The EvalStream contains a non-expressible TupleStream - it cannot be converted to an expression");
        }

        return expression;
    }

    @Override
    public Explanation toExplanation(StreamFactory streamFactory) throws IOException {
        return this.stream.toExplanation(streamFactory);
    }

}
