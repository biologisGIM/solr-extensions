package com.biologis.solr.client.solrj.io.util;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.stream.TupleStream;

import java.io.IOException;
import java.util.HashSet;

/**
 * Worker is a sorter (See SortStream)
 * This one also filters the stream by Unique (by uniqueBy field)
 */
public class UniqueWorker extends Worker{

    private String uniqueBy;

    public UniqueWorker(String uniqueBy, StreamComparator comparator){
        super(comparator);
        this.uniqueBy = uniqueBy;
    }

    public void readStream(TupleStream stream) throws IOException {
        HashSet<String> processedNodes = new HashSet<>();
        Tuple tuple = stream.read();
        String curNode;

        while(!tuple.EOF){
            curNode = (String)tuple.get(this.uniqueBy);

            if (!processedNodes.contains(curNode)){
                tuples.add(tuple);
                processedNodes.add(curNode);
            }

            tuple = stream.read();
        }

        eofTuple = tuple;
    }


}
