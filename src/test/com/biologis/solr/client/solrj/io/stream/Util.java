package com.biologis.solr.client.solrj.io.stream;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.SolrStream;
import org.apache.solr.client.solrj.io.stream.StreamContext;
import org.apache.solr.client.solrj.io.stream.TupleStream;
import org.apache.solr.common.params.ModifiableSolrParams;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class Util {

    private static String url = "http://localhost:8983/solr/kmm";


    public List<Tuple> getTuples(String streamingExp) throws IOException {
        TupleStream tupleStream = this.getTupleStream(streamingExp);

        // TODO sort the stream!

        //Parse TupleStream to List
        List<Tuple> tupleList = new LinkedList<Tuple>();

        tupleStream.open();

        Tuple t;
        while ( !(t = tupleStream.read()).EOF ){
            tupleList.add(t);
        }

        tupleStream.close();

        return tupleList;
    }

    public TupleStream getTupleStream(String streamingExp) throws IOException {
        ModifiableSolrParams params = new ModifiableSolrParams();
        params.set("expr", streamingExp);
        params.set("qt", "/stream");

        TupleStream tupleStream = new SolrStream(this.url, params);
        StreamContext context = new StreamContext();
        tupleStream.setStreamContext(context);

        return tupleStream;
    }

    public boolean areTuplesEqual(Tuple one, Tuple two){
        if (one.fields.keySet().equals(two.fields.keySet())) {
            String key;
            Iterator it = one.fields.keySet().iterator();

            while (it.hasNext()){
                key = (String)it.next();
                if (!one.get(key).equals(two.get(key))){
                    return false;
                }
            }

            return true;
        }

        return false;
    }

}
