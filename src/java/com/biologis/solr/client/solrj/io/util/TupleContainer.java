package com.biologis.solr.client.solrj.io.util;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.stream.StreamContext;
import org.apache.solr.client.solrj.io.stream.TupleStream;
import org.apache.solr.client.solrj.io.stream.expr.Explanation;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * So that a List of Tuples can be passed to default streams (e.g. InnerJoinStream); for example a list of tuples sorted with a Worker
 */
public class TupleContainer extends TupleStream {

    private LinkedList<Tuple> tuples;
    private Tuple eofTuple;
    private StreamComparator comparator;

    public TupleContainer(LinkedList<Tuple> tuples){
        this.tuples = tuples;
    }

    public TupleContainer(Worker worker){
        this.tuples = new LinkedList<Tuple>();
        this.comparator = worker.comparator;

        Tuple t = worker.read();
        while(!t.EOF){
            this.tuples.add(t);
            t = worker.read();
        }
        this.eofTuple = t;
    }

    @Override
    public void setStreamContext(StreamContext context) {

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
        if (this.tuples.isEmpty() ){
            return this.eofTuple;
        }
        return this.tuples.removeFirst();
    }

    @Override
    public StreamComparator getStreamSort() {
        return this.comparator;
    }

    @Override
    public Explanation toExplanation(StreamFactory factory) throws IOException {
        return null;
    }
}
