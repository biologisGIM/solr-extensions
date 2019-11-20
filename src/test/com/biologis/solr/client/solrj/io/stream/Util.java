package com.biologis.solr.client.solrj.io.stream;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.SolrStream;
import org.apache.solr.client.solrj.io.stream.StreamContext;
import org.apache.solr.client.solrj.io.stream.TupleStream;
import org.apache.solr.common.params.ModifiableSolrParams;

import java.util.LinkedList;
import java.util.List;

public class Tempo {

    private static String url = "http://localhost:8983/solr/kmm";


    public List<Tuple> getTuples(String streamingExp){
        ModifiableSolrParams oldParams = new ModifiableSolrParams();
        oldParams.set("expr", oldQueryString);
        oldParams.set("qt", "/stream");

        TupleStream oldTupleStream = new SolrStream(url, oldParams);
        StreamContext context = new StreamContext();
        oldTupleStream.setStreamContext(context);

        // New streaming expression
        ModifiableSolrParams newParams = new ModifiableSolrParams();
        newParams.set("expr", newQueryString);
        newParams.set("qt", "/stream");

        TupleStream newTupleStream = new SolrStream(url, newParams);
        StreamContext context2 = new StreamContext();
        oldTupleStream.setStreamContext(context2);

        //Parse TupleStream to List
        List<Tuple> oldTupleList = new LinkedList<Tuple>();
        List<Tuple> newTupleList = new LinkedList<Tuple>();

        try{
            oldTupleStream.open();

            Tuple t;
            while ( !(t = oldTupleStream.read()).EOF ){
                oldTupleList.add(t);
            }

            oldTupleStream.close();

            newTupleStream.open();

            while ( !(t = newTupleStream.read()).EOF ){
                newTupleList.add(t);
            }
        }

    }
}
