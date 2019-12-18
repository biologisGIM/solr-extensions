package com.biologis.solr.client.solrj.io.util;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.stream.TupleStream;

import java.io.IOException;
import java.util.LinkedList;

/**
 * A sorter (See SortStream)
 */
public class Worker {

    protected final LinkedList<Tuple> tuples = new LinkedList<>();
    protected final StreamComparator comparator;
    protected Tuple eofTuple;

    public Worker(StreamComparator comparator){
        this.comparator = comparator;
    }

    public void readStream(TupleStream stream) throws IOException{
        Tuple tuple = stream.read();

        while(!tuple.EOF){
            tuples.add(tuple);
            tuple = stream.read();
        }

        this.eofTuple = tuple;
    }

    public void sort() {
        this.tuples.sort(this.comparator);
    }

    public Tuple read() {
        if(this.tuples.isEmpty()){
            return this.eofTuple;
        }

        return this.tuples.removeFirst();
    }

    public StreamComparator getComparator(){
        return this.comparator;
    }

}
