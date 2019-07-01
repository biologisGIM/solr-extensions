package com.biologis.solr.client.solrj.io.stream;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.io.SolrClientCache;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.StreamContext;
import org.apache.solr.client.solrj.io.stream.TupleStream;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionParser;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@LuceneTestCase.Slow
@SolrTestCaseJ4.SuppressSSL
@LuceneTestCase.SuppressCodecs({"Lucene3x", "Lucene40","Lucene41","Lucene42","Lucene45","Lucene70"})
public class SearchAllTest extends SolrCloudTestCase {

    private static final String COLLECTIONORALIAS = "collection1";
    private static final int TIMEOUT = DEFAULT_TIMEOUT;
    private static final String id = "id";

    private static boolean useAlias;

    @BeforeClass
    public static void setupCluster() throws Exception {
        configureCluster(4)
                .addConfig("conf", getFile("configsets").toPath().resolve("conf"))

                .configure();

        String collection;
        useAlias = random().nextBoolean();
        if (useAlias) {
            collection = COLLECTIONORALIAS + "_collection";
        } else {
            collection = COLLECTIONORALIAS;
        }

        CollectionAdminRequest.createCollection(collection, "conf", 2, 1).process(cluster.getSolrClient());
        cluster.waitForActiveCollection(collection, 2, 2);
        if (useAlias) {
            CollectionAdminRequest.createAlias(COLLECTIONORALIAS, collection).process(cluster.getSolrClient());
        }
    }

    @Before
    public void cleanIndex() throws Exception {
        new UpdateRequest()
                .deleteByQuery("*:*")
                .commit(cluster.getSolrClient(), COLLECTIONORALIAS);
    }

    @Test
    public void testSearchAll() throws Exception {

        new UpdateRequest()
                .add(id, "0", "a_s", "hello0", "a_i", "0", "a_f", "0")
                .add(id, "2", "a_s", "hello2", "a_i", "2", "a_f", "0")
                .add(id, "3", "a_s", "hello3", "a_i", "3", "a_f", "3")
                .add(id, "4", "a_s", "hello4", "a_i", "4", "a_f", "4")
                .add(id, "1", "a_s", "hello1", "a_i", "1", "a_f", "1")
                .commit(cluster.getSolrClient(), COLLECTIONORALIAS);

        StreamFactory factory = new StreamFactory().withCollectionZkHost(COLLECTIONORALIAS, cluster.getZkServer().getZkAddress());
        StreamExpression expression;
        SearchAll stream;
        List<Tuple> tuples;
        StreamContext streamContext = new StreamContext();
        SolrClientCache solrClientCache = new SolrClientCache();
        streamContext.setSolrClientCache(solrClientCache);

        try {
            expression = StreamExpressionParser.parse("searchAll(" + COLLECTIONORALIAS + ", q=*:*, fl=\"id,a_s,a_i,a_f\", sort=\"a_f asc, a_i asc\")");
            stream = new SearchAll(expression, factory);
            stream.setStreamContext(streamContext);
            tuples = getTuples(stream);

            assert (tuples.size() == 5);
            assertOrder(tuples, 0, 2, 1, 3, 4);
            assertString(tuples.get(0), "a_s", "hello0");
            assertString(tuples.get(4),"a_s", "hello4");

        } finally {
            solrClientCache.close();
        }
    }

    /*-----------------------------SIDE-STUFF-----------------------------*/

    protected List<Tuple> getTuples(TupleStream tupleStream) throws IOException {
        List<Tuple> tuples = new ArrayList<Tuple>();

        try {
            tupleStream.open();
            for (Tuple t = tupleStream.read(); !t.EOF; t = tupleStream.read()) {
                tuples.add(t);
            }
        } finally {
            tupleStream.close();
        }
        return tuples;
    }
    protected boolean assertOrder(List<Tuple> tuples, int... ids) throws Exception {
        return assertOrderOf(tuples, "id", ids);
    }
    protected boolean assertOrderOf(List<Tuple> tuples, String fieldName, int... ids) throws Exception {
        int i = 0;
        for(int val : ids) {
            Tuple t = tuples.get(i);
            String tip = t.getString(fieldName);
            if(!tip.equals(Integer.toString(val))) {
                throw new Exception("Found value:"+tip+" expecting:"+val);
            }
            ++i;
        }
        return true;
    }

    public boolean assertLong(Tuple tuple, String fieldName, long l) throws Exception {
        long lv = (long)tuple.get(fieldName);
        if(lv != l) {
            throw new Exception("Longs not equal:"+l+" : "+lv);
        }

        return true;
    }

    public boolean assertString(Tuple tuple, String fieldName, String expected) throws Exception {
        String actual = (String)tuple.get(fieldName);

        if( (null == expected && null != actual) ||
                (null != expected && null == actual) ||
                (null != expected && !expected.equals(actual))){
            throw new Exception("Longs not equal:"+expected+" : "+actual);
        }

        return true;
    }

    public boolean assertDouble(Tuple tuple, String fieldName, double d) throws Exception {
        double dv = tuple.getDouble(fieldName);
        if(dv != d) {
            throw new Exception("Doubles not equal:"+d+" : "+dv);
        }

        return true;
    }

}