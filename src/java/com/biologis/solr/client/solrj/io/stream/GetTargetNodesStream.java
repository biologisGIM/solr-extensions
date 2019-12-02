package com.biologis.solr.client.solrj.io.stream;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.FieldComparator;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.stream.*;
import org.apache.solr.client.solrj.io.stream.expr.*;

import java.io.IOException;
import java.util.*;

public class GetTargetNodesStream extends TupleStream implements Expressible {

    private TupleStream stream;
    private StreamComparator comparator;
    private GetTargetNodesStream.Worker worker;
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

        worker = new GetTargetNodesStream.Worker() {

            private LinkedList<Tuple> tuples = new LinkedList<Tuple>();
            private Tuple eofTuple;

            public void readStream(TupleStream stream) throws IOException {
                HashSet<String> processedNodes = new HashSet<>();
                Tuple tuple = stream.read();
                String curNode;

                while(!tuple.EOF){
                    curNode = (String)tuple.get("node");

                    if (!processedNodes.contains(curNode)){
                        tuples.add(tuple);
                        processedNodes.add(curNode);
                    }

                    tuple = stream.read();
                }

                eofTuple = tuple;
            }

            public void sort() {
                tuples.sort(comparator);
            }

            public Tuple read() {
                if(tuples.isEmpty()){
                    return eofTuple;
                }

                return tuples.removeFirst();
            }
        };


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
    }

    @Override
    public List<TupleStream> children() {
        return this.stream.children();
    }

    @Override
    public void open() throws IOException {
        this.stream.open();

        worker.readStream(stream);
        worker.sort();
    }

    @Override
    public void close() throws IOException {
        this.stream.close();
    }

    @Override
    public Tuple read() throws IOException {
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
        return null;
    }

    @Override
    public Explanation toExplanation(StreamFactory streamFactory) throws IOException {
        return this.stream.toExplanation(streamFactory);
    }

    private interface Worker {
        void readStream(TupleStream var1) throws IOException;

        void sort();

        Tuple read();
    }
}
