package com.biologis.solr.client.solrj.io.stream;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.stream.SortStream;
import org.apache.solr.client.solrj.io.stream.StreamContext;
import org.apache.solr.client.solrj.io.stream.TupleStream;
import org.apache.solr.client.solrj.io.stream.expr.*;

import java.io.IOException;
import java.util.List;

public class GetTargetNodes extends TupleStream implements Expressible {

    private TupleStream incomingStream;

    public GetTargetNodes(TupleStream stream, StreamComparator comp) throws IOException {
    }

    public GetTargetNodes(StreamExpression expression, StreamFactory factory) throws IOException {
    }


    @Override
    public void setStreamContext(StreamContext streamContext) {

    }

    @Override
    public List<TupleStream> children() {
        return null;
    }

    @Override
    public void open() throws IOException {

    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public Tuple read() throws IOException {
        return null;
    }

    @Override
    public StreamComparator getStreamSort() {
        return null;
    }

    @Override
    public StreamExpressionParameter toExpression(StreamFactory streamFactory) throws IOException {
        return null;
    }

    @Override
    public Explanation toExplanation(StreamFactory streamFactory) throws IOException {
        return null;
    }
}
