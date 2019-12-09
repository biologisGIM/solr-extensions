package com.biologis.solr.client.solrj.io.util;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.stream.TupleStream;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;

/**
 * Worker is a sorter (See SortStream)
 * This one also filters the stream by Unique (by uniqueBy field) and Having (having havingWhat equal to havingCompareTo)
 */
public class HavingUniqueWorker extends Worker {

    private final String uniqueBy;
    private final String havingWhat;
    private final String havingCompareTo;

    public HavingUniqueWorker(String uniqueBy, String having, String havingCompareTo, StreamComparator comparator){
        super(comparator);
        this.uniqueBy = uniqueBy;
        this.havingWhat = having;
        this.havingCompareTo = havingCompareTo;
    }

    public void readStream(TupleStream stream) throws IOException {
        HashSet<String> processedNodes = new HashSet<>();
        Tuple tuple = stream.read();
        String curNode;

        while(!tuple.EOF){
            curNode = (String)tuple.get(this.uniqueBy);

            if (!processedNodes.contains(curNode) && ((List)tuple.get(this.havingCompareTo)).contains(tuple.get(this.havingWhat))){
                tuple.put(this.havingCompareTo, tuple.get(this.havingWhat));
                tuples.add(tuple);
                processedNodes.add(curNode);
            }

            tuple = stream.read();
        }

        eofTuple = tuple;
    }
}
