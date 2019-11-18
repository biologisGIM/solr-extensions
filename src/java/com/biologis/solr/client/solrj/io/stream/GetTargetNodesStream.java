package com.biologis.solr.client.solrj.io.stream;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.FieldComparator;
import org.apache.solr.client.solrj.io.comp.MultipleFieldComparator;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.eq.FieldEqualitor;
import org.apache.solr.client.solrj.io.eq.MultipleFieldEqualitor;
import org.apache.solr.client.solrj.io.eq.StreamEqualitor;
import org.apache.solr.client.solrj.io.stream.*;
import org.apache.solr.client.solrj.io.stream.expr.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

public class GetTargetNodesStream extends TupleStream implements Expressible {

    private TupleStream stream;
    private StreamFactory factory;

    public GetTargetNodesStream(TupleStream stream) throws IOException{
        this.init(stream, null);
    }

    public GetTargetNodesStream(StreamExpression expression, StreamFactory factory) throws IOException{
        List<StreamExpression> streamExpressions = factory.getExpressionOperandsRepresentingTypes(expression, new Class[]{Expressible.class, TupleStream.class});

        if (expression.getParameters().size() != streamExpressions.size()) {
            throw new IOException(String.format(Locale.ROOT, "Invalid expression %s - unknown operands found", expression));

        } else if (streamExpressions.size() != 1) {
            throw new IOException(String.format(Locale.ROOT, "Invalid expression %s - expecting two streams but found %d (must be TupleStream types)", expression, streamExpressions.size()));

        } else {
            this.init(factory.constructStream((StreamExpression)streamExpressions.get(0)), factory);

        }
    }

    public void init(TupleStream stream, StreamFactory factory) throws IOException{
        this.stream = stream;
        this.factory = factory;
        if (factory == null){
            this.factory = new StreamFactory();
        }
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
        //Sort by node asc
        StreamComparator byNodeAsc = this.factory.constructComparator("node asc", FieldComparator.class);
        SortStream innerSort = new SortStream(this.stream, byNodeAsc);

        innerSort.open();

        //Unique over node
        UniqueStream unique = new UniqueStream(innerSort, this.convertToEqualitor(byNodeAsc));

        //Select
        //SelectStream select = new SelectStream(unique, this.getFieldList());

    }

    private String getSelectOptions(){
        List<String> options = new ArrayList<String>();
        options.add("endpoint_id");
        options.add("endpoint_name");
        options.add("endpoint_type");

        options.add("endpoint_name");
        options.add("endpoint_name");
        options.add("endpoint_name");
        options.add("endpoint_name");
        options.add("endpoint_name");
        return "";
    }

    private StreamEqualitor convertToEqualitor(StreamComparator comp) {
        if (!(comp instanceof MultipleFieldComparator)) {
            FieldComparator fComp = (FieldComparator)comp;
            return new FieldEqualitor(fComp.getLeftFieldName(), fComp.getRightFieldName());
        } else {
            MultipleFieldComparator mComp = (MultipleFieldComparator)comp;
            StreamEqualitor[] eqs = new StreamEqualitor[mComp.getComps().length];

            for(int idx = 0; idx < mComp.getComps().length; ++idx) {
                eqs[idx] = this.convertToEqualitor(mComp.getComps()[idx]);
            }

            return new MultipleFieldEqualitor(eqs);
        }
    }

    @Override
    public void close() throws IOException {
        this.stream.close();
    }

    @Override
    public Tuple read() throws IOException {
        return null;
    }

    @Override
    public StreamComparator getStreamSort() {
        return this.stream.getStreamSort();
    }

    @Override
    public StreamExpressionParameter toExpression(StreamFactory streamFactory) throws IOException {
        return null;
    }

    @Override
    public Explanation toExplanation(StreamFactory streamFactory) throws IOException {
        return this.stream.toExplanation(streamFactory);
    }
}
