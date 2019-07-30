package com.biologis.solr.client.solrj.io.graph;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.client.solrj.io.SolrClientCache;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.ComparatorOrder;
import org.apache.solr.client.solrj.io.comp.FieldComparator;
import org.apache.solr.client.solrj.io.stream.CloudSolrStream;
import org.apache.solr.client.solrj.io.stream.HashJoinStream;
import org.apache.solr.client.solrj.io.stream.StreamContext;
import org.apache.solr.client.solrj.io.stream.TupleStream;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.apache.solr.client.solrj.io.stream.metrics.CountMetric;
import org.apache.solr.client.solrj.io.stream.metrics.MaxMetric;
import org.apache.solr.client.solrj.io.stream.metrics.MeanMetric;
import org.apache.solr.client.solrj.io.stream.metrics.MinMetric;
import org.apache.solr.client.solrj.io.stream.metrics.SumMetric;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.cloud.AbstractDistribZkTestBase;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

@Slow
@LuceneTestCase.SuppressCodecs({"Lucene3x", "Lucene40","Lucene41","Lucene42","Lucene45"})
public class NodesNoCycleTest extends SolrCloudTestCase {

    private static final String COLLECTION = "collection1";

    private static final String id = "id";

    private static final int TIMEOUT = 30;

    @BeforeClass
    public static void setupCluster() throws Exception {
        configureCluster(2)
                .addConfig("conf", getFile("configsets").toPath().resolve("conf"))
                .configure();

        CollectionAdminRequest.createCollection(COLLECTION, "conf", 2, 1).process(cluster.getSolrClient());
        AbstractDistribZkTestBase.waitForRecoveriesToFinish(COLLECTION, cluster.getSolrClient().getZkStateReader(),
                false, true, TIMEOUT);
    }

    @Before
    public void cleanIndex() throws Exception {
        new UpdateRequest()
                .deleteByQuery("*:*")
                .commit(cluster.getSolrClient(), COLLECTION);
    }

    @Test
    public void testNodeNoCycle() throws Exception {

        new UpdateRequest()
                .add(id, "0", "basket_s", "basket1", "product_s", "product1", "price_f", "20")
                .add(id, "1", "basket_s", "basket1", "product_s", "product3", "price_f", "30")
                .add(id, "2", "basket_s", "basket1", "product_s", "product5", "price_f", "1")
                .add(id, "3", "basket_s", "basket2", "product_s", "product1", "price_f", "2")
                .add(id, "4", "basket_s", "basket2", "product_s", "product6", "price_f", "5")
                .add(id, "5", "basket_s", "basket2", "product_s", "product7", "price_f", "10")
                .add(id, "6", "basket_s", "basket3", "product_s", "product4", "price_f", "20")
                .add(id, "7", "basket_s", "basket3", "product_s", "product3", "price_f", "10")
                .add(id, "8", "basket_s", "basket3", "product_s", "product1", "price_f", "10")
                .add(id, "9", "basket_s", "basket4", "product_s", "product4", "price_f", "40")
                .add(id, "10", "basket_s", "basket4", "product_s", "product3", "price_f", "10")
                .add(id, "11", "basket_s", "basket4", "product_s", "product1", "price_f", "10")
                .commit(cluster.getSolrClient(), COLLECTION);

        List<Tuple> tuples = null;
        Set<String> paths = null;
        NodesNoCycle stream = null;
        StreamContext context = new StreamContext();
        SolrClientCache cache = new SolrClientCache();
        context.setSolrClientCache(cache);

        StreamFactory factory = new StreamFactory()
                .withCollectionZkHost("collection1", cluster.getZkServer().getZkAddress())
                .withFunctionName("nodesNoCycle", NodesNoCycle.class)
                .withFunctionName("search", CloudSolrStream.class)
                .withFunctionName("count", CountMetric.class)
                .withFunctionName("avg", MeanMetric.class)
                .withFunctionName("sum", SumMetric.class)
                .withFunctionName("min", MinMetric.class)
                .withFunctionName("max", MaxMetric.class);

        String expr = "nodesNoCycle(collection1, " +
                "walk=\"product1->product_s\"," +
                "gather=\"basket_s\")";

        stream = (NodesNoCycle) factory.constructStream(expr);
        stream.setStreamContext(context);

        tuples = getTuples(stream);

        Collections.sort(tuples, new FieldComparator("node", ComparatorOrder.ASCENDING));
        assertTrue(tuples.size() == 4);
        assertTrue(tuples.get(0).getString("node").equals("basket1"));
        assertTrue(tuples.get(1).getString("node").equals("basket2"));
        assertTrue(tuples.get(2).getString("node").equals("basket3"));
        assertTrue(tuples.get(3).getString("node").equals("basket4"));


        //Test maxDocFreq param
        String docFreqExpr = "nodesNoCycle(collection1, " +
                "walk=\"product1, product7->product_s\"," +
                "maxDocFreq=\"2\","+
                "gather=\"basket_s\")";

        stream = (NodesNoCycle) factory.constructStream(docFreqExpr);
        stream.setStreamContext(context);

        tuples = getTuples(stream);
        Collections.sort(tuples, new FieldComparator("node", ComparatorOrder.ASCENDING));
        assertTrue(tuples.size() == 1);
        assertTrue(tuples.get(0).getString("node").equals("basket2"));



        String expr2 = "nodesNoCycle(collection1, " +
                expr+","+
                "walk=\"node->basket_s\"," +
                "gather=\"product_s\", count(*), avg(price_f), sum(price_f), min(price_f), max(price_f))";

        stream = (NodesNoCycle) factory.constructStream(expr2);

        context = new StreamContext();
        context.setSolrClientCache(cache);

        stream.setStreamContext(context);


        tuples = getTuples(stream);

        Collections.sort(tuples, new FieldComparator("node", ComparatorOrder.ASCENDING));


        assertTrue(tuples.size() == 5);


        assertTrue(tuples.get(0).getString("node").equals("product3"));
        assertTrue(tuples.get(0).getDouble("count(*)").equals(3.0D));

        assertTrue(tuples.get(1).getString("node").equals("product4"));
        assertTrue(tuples.get(1).getDouble("count(*)").equals(2.0D));
        assertTrue(tuples.get(1).getDouble("avg(price_f)").equals(30.0D));
        assertTrue(tuples.get(1).getDouble("sum(price_f)").equals(60.0D));
        assertTrue(tuples.get(1).getDouble("min(price_f)").equals(20.0D));
        assertTrue(tuples.get(1).getDouble("max(price_f)").equals(40.0D));

        assertTrue(tuples.get(2).getString("node").equals("product5"));
        assertTrue(tuples.get(2).getDouble("count(*)").equals(1.0D));
        assertTrue(tuples.get(3).getString("node").equals("product6"));
        assertTrue(tuples.get(3).getDouble("count(*)").equals(1.0D));
        assertTrue(tuples.get(4).getString("node").equals("product7"));
        assertTrue(tuples.get(4).getDouble("count(*)").equals(1.0D));

        //Test list of root nodes
        expr = "nodesNoCycle(collection1, " +
                "walk=\"product4, product7->product_s\"," +
                "gather=\"basket_s\")";

        stream = (NodesNoCycle) factory.constructStream(expr);

        context = new StreamContext();
        context.setSolrClientCache(cache);
        stream.setStreamContext(context);
        tuples = getTuples(stream);
        Collections.sort(tuples, new FieldComparator("node", ComparatorOrder.ASCENDING));
        assertTrue(tuples.size() == 3);
        assertTrue(tuples.get(0).getString("node").equals("basket2"));
        assertTrue(tuples.get(1).getString("node").equals("basket3"));
        assertTrue(tuples.get(2).getString("node").equals("basket4"));

        //Test with negative filter query

        expr = "nodesNoCycle(collection1, " +
                "walk=\"product4, product7->product_s\"," +
                "gather=\"basket_s\", fq=\"-basket_s:basket4\")";

        stream = (NodesNoCycle) factory.constructStream(expr);

        context = new StreamContext();
        context.setSolrClientCache(cache);
        stream.setStreamContext(context);
        tuples = getTuples(stream);

        Collections.sort(tuples, new FieldComparator("node", ComparatorOrder.ASCENDING));
        assertTrue(tuples.size() == 2);
        assertTrue(tuples.get(0).getString("node").equals("basket2"));
        assertTrue(tuples.get(1).getString("node").equals("basket3"));

        cache.close();

    }

    @Test
    public void testNodesNoCycleFriendsStream() throws Exception {

        new UpdateRequest()
                .add(id, "0", "from_s", "bill", "to_s", "jim", "message_t", "Hello jim")
                .add(id, "1", "from_s", "bill", "to_s", "sam", "message_t", "Hello sam")
                .add(id, "2", "from_s", "bill", "to_s", "max", "message_t", "Hello max")
                .add(id, "3", "from_s", "max",  "to_s", "kip", "message_t", "Hello kip")
                .add(id, "4", "from_s", "sam",  "to_s", "steve", "message_t", "Hello steve")
                .add(id, "5", "from_s", "jim",  "to_s", "ann", "message_t", "Hello steve")
                .commit(cluster.getSolrClient(), COLLECTION);

        List<Tuple> tuples = null;
        NodesNoCycle stream = null;
        StreamContext context = new StreamContext();
        SolrClientCache cache = new SolrClientCache();
        context.setSolrClientCache(cache);

        StreamFactory factory = new StreamFactory()
                .withCollectionZkHost("collection1", cluster.getZkServer().getZkAddress())
                .withFunctionName("nodesNoCycle", NodesNoCycle.class)
                .withFunctionName("search", CloudSolrStream.class)
                .withFunctionName("count", CountMetric.class)
                .withFunctionName("hashJoin", HashJoinStream.class)
                .withFunctionName("avg", MeanMetric.class)
                .withFunctionName("sum", SumMetric.class)
                .withFunctionName("min", MinMetric.class)
                .withFunctionName("max", MaxMetric.class);

        String expr = "nodesNoCycle(collection1, " +
                "walk=\"bill->from_s\"," +
                "gather=\"to_s\")";

        stream = (NodesNoCycle) factory.constructStream(expr);
        stream.setStreamContext(context);

        tuples = getTuples(stream);

        Collections.sort(tuples, new FieldComparator("node", ComparatorOrder.ASCENDING));
        assertTrue(tuples.size() == 3);
        assertTrue(tuples.get(0).getString("node").equals("jim"));
        assertTrue(tuples.get(1).getString("node").equals("max"));
        assertTrue(tuples.get(2).getString("node").equals("sam"));

        //Test scatter branches, leaves and trackTraversal

        expr = "nodesNoCycle(collection1, " +
                "walk=\"bill->from_s\"," +
                "gather=\"to_s\","+
                "scatter=\"branches, leaves\", trackTraversal=\"true\")";

        stream = (NodesNoCycle) factory.constructStream(expr);
        context = new StreamContext();
        context.setSolrClientCache(cache);
        stream.setStreamContext(context);

        tuples = getTuples(stream);

        Collections.sort(tuples, new FieldComparator("node", ComparatorOrder.ASCENDING));
        assertTrue(tuples.size() == 4);
        assertTrue(tuples.get(0).getString("node").equals("bill"));
        assertTrue(tuples.get(0).getLong("level").equals(0L));
        assertTrue(tuples.get(0).getStrings("ancestors").size() == 0);
        assertTrue(tuples.get(1).getString("node").equals("jim"));
        assertTrue(tuples.get(1).getLong("level").equals(1L));
        List<String> ancestors = tuples.get(1).getStrings("ancestors");
        System.out.println("##################### Ancestors:"+ancestors);
        assert(ancestors.size() == 1);
        assert(ancestors.get(0).equals("bill"));

        assertTrue(tuples.get(2).getString("node").equals("max"));
        assertTrue(tuples.get(2).getLong("level").equals(1L));
        ancestors = tuples.get(2).getStrings("ancestors");
        assert(ancestors.size() == 1);
        assert(ancestors.get(0).equals("bill"));

        assertTrue(tuples.get(3).getString("node").equals("sam"));
        assertTrue(tuples.get(3).getLong("level").equals(1L));
        ancestors = tuples.get(3).getStrings("ancestors");
        assert(ancestors.size() == 1);
        assert(ancestors.get(0).equals("bill"));

        // Test query root

        expr = "nodesNoCycle(collection1, " +
                "search(collection1, q=\"message_t:jim\", fl=\"from_s\", sort=\"from_s asc\"),"+
                "walk=\"from_s->from_s\"," +
                "gather=\"to_s\")";

        stream = (NodesNoCycle) factory.constructStream(expr);
        context = new StreamContext();
        context.setSolrClientCache(cache);
        stream.setStreamContext(context);

        tuples = getTuples(stream);

        Collections.sort(tuples, new FieldComparator("node", ComparatorOrder.ASCENDING));
        assertTrue(tuples.size() == 3);
        assertTrue(tuples.get(0).getString("node").equals("jim"));
        assertTrue(tuples.get(1).getString("node").equals("max"));
        assertTrue(tuples.get(2).getString("node").equals("sam"));


        // Test query root scatter branches

        expr = "nodesNoCycle(collection1, " +
                "search(collection1, q=\"message_t:jim\", fl=\"from_s\", sort=\"from_s asc\"),"+
                "walk=\"from_s->from_s\"," +
                "gather=\"to_s\", scatter=\"branches, leaves\")";

        stream = (NodesNoCycle) factory.constructStream(expr);
        context = new StreamContext();
        context.setSolrClientCache(cache);
        stream.setStreamContext(context);

        tuples = getTuples(stream);

        Collections.sort(tuples, new FieldComparator("node", ComparatorOrder.ASCENDING));
        assertTrue(tuples.size() == 4);
        assertTrue(tuples.get(0).getString("node").equals("bill"));
        assertTrue(tuples.get(0).getLong("level").equals(0L));
        assertTrue(tuples.get(1).getString("node").equals("jim"));
        assertTrue(tuples.get(1).getLong("level").equals(1L));
        assertTrue(tuples.get(2).getString("node").equals("max"));
        assertTrue(tuples.get(2).getLong("level").equals(1L));
        assertTrue(tuples.get(3).getString("node").equals("sam"));
        assertTrue(tuples.get(3).getLong("level").equals(1L));

        expr = "nodesNoCycle(collection1, " +
                "search(collection1, q=\"message_t:jim\", fl=\"from_s\", sort=\"from_s asc\"),"+
                "walk=\"from_s->from_s\"," +
                "gather=\"to_s\")";

        String expr2 = "nodesNoCycle(collection1, " +
                expr+","+
                "walk=\"node->from_s\"," +
                "gather=\"to_s\")";

        stream = (NodesNoCycle) factory.constructStream(expr2);
        context = new StreamContext();
        context.setSolrClientCache(cache);
        stream.setStreamContext(context);

        tuples = getTuples(stream);
        Collections.sort(tuples, new FieldComparator("node", ComparatorOrder.ASCENDING));

        assertTrue(tuples.size() == 3);
        assertTrue(tuples.get(0).getString("node").equals("ann"));
        assertTrue(tuples.get(1).getString("node").equals("kip"));
        assertTrue(tuples.get(2).getString("node").equals("steve"));


        //Test two traversals in the same expression
        String expr3 = "hashJoin("+expr2+", hashed="+expr2+", on=\"node\")";

        HashJoinStream hstream = (HashJoinStream)factory.constructStream(expr3);
        context = new StreamContext();
        context.setSolrClientCache(cache);
        hstream.setStreamContext(context);

        tuples = getTuples(hstream);
        Collections.sort(tuples, new FieldComparator("node", ComparatorOrder.ASCENDING));

        assertTrue(tuples.size() == 3);
        assertTrue(tuples.get(0).getString("node").equals("ann"));
        assertTrue(tuples.get(1).getString("node").equals("kip"));
        assertTrue(tuples.get(2).getString("node").equals("steve"));

        //=================================


        expr = "nodesNoCycle(collection1, " +
                "search(collection1, q=\"message_t:jim\", fl=\"from_s\", sort=\"from_s asc\"),"+
                "walk=\"from_s->from_s\"," +
                "gather=\"to_s\")";

        expr2 = "nodesNoCycle(collection1, " +
                expr+","+
                "walk=\"node->from_s\"," +
                "gather=\"to_s\", scatter=\"branches, leaves\")";

        stream = (NodesNoCycle) factory.constructStream(expr2);
        context = new StreamContext();
        context.setSolrClientCache(cache);
        stream.setStreamContext(context);

        tuples = getTuples(stream);
        Collections.sort(tuples, new FieldComparator("node", ComparatorOrder.ASCENDING));


        assertTrue(tuples.size() == 7);
        assertTrue(tuples.get(0).getString("node").equals("ann"));
        assertTrue(tuples.get(0).getLong("level").equals(2L));
        assertTrue(tuples.get(1).getString("node").equals("bill"));
        assertTrue(tuples.get(1).getLong("level").equals(0L));
        assertTrue(tuples.get(2).getString("node").equals("jim"));
        assertTrue(tuples.get(2).getLong("level").equals(1L));
        assertTrue(tuples.get(3).getString("node").equals("kip"));
        assertTrue(tuples.get(3).getLong("level").equals(2L));
        assertTrue(tuples.get(4).getString("node").equals("max"));
        assertTrue(tuples.get(4).getLong("level").equals(1L));
        assertTrue(tuples.get(5).getString("node").equals("sam"));
        assertTrue(tuples.get(5).getLong("level").equals(1L));
        assertTrue(tuples.get(6).getString("node").equals("steve"));
        assertTrue(tuples.get(6).getLong("level").equals(2L));

        //Add a cycle from jim to bill
        new UpdateRequest()
                .add(id, "6", "from_s", "jim", "to_s", "bill", "message_t", "Hello steve")
                .add(id, "7", "from_s", "sam", "to_s", "bill", "message_t", "Hello steve")
                .commit(cluster.getSolrClient(), COLLECTION);

        expr = "nodesNoCycle(collection1, " +
                "search(collection1, q=\"message_t:jim\", fl=\"from_s\", sort=\"from_s asc\"),"+
                "walk=\"from_s->from_s\"," +
                "gather=\"to_s\", trackTraversal=\"true\")";

        expr2 = "nodesNoCycle(collection1, " +
                expr+","+
                "walk=\"node->from_s\"," +
                "gather=\"to_s\", scatter=\"branches, leaves\", trackTraversal=\"true\")";

        stream = (NodesNoCycle) factory.constructStream(expr2);
        context = new StreamContext();
        context.setSolrClientCache(cache);
        stream.setStreamContext(context);

        tuples = getTuples(stream);
        Collections.sort(tuples, new FieldComparator("node", ComparatorOrder.ASCENDING));

        assertTrue(tuples.size() == 7);
        assertTrue(tuples.get(0).getString("node").equals("ann"));
        assertTrue(tuples.get(0).getLong("level").equals(2L));
        //Bill should now have one ancestor
        assertTrue(tuples.get(1).getString("node").equals("bill"));
        assertTrue(tuples.get(1).getLong("level").equals(0L));
        assertTrue(tuples.get(1).getStrings("ancestors").size() == 2);
        List<String> anc = tuples.get(1).getStrings("ancestors");

        Collections.sort(anc);
        assertTrue(anc.get(0).equals("jim"));
        assertTrue(anc.get(1).equals("sam"));

        assertTrue(tuples.get(2).getString("node").equals("jim"));
        assertTrue(tuples.get(2).getLong("level").equals(1L));
        assertTrue(tuples.get(3).getString("node").equals("kip"));
        assertTrue(tuples.get(3).getLong("level").equals(2L));
        assertTrue(tuples.get(4).getString("node").equals("max"));
        assertTrue(tuples.get(4).getLong("level").equals(1L));
        assertTrue(tuples.get(5).getString("node").equals("sam"));
        assertTrue(tuples.get(5).getLong("level").equals(1L));
        assertTrue(tuples.get(6).getString("node").equals("steve"));
        assertTrue(tuples.get(6).getLong("level").equals(2L));

        cache.close();

    }

    protected List<Tuple> getTuples(TupleStream tupleStream) throws IOException {
        tupleStream.open();
        List<Tuple> tuples = new ArrayList();
        for(Tuple t = tupleStream.read(); !t.EOF; t = tupleStream.read()) {
            tuples.add(t);
        }
        tupleStream.close();
        return tuples;
    }

    protected boolean assertOrderOf(List<Tuple> tuples, String fieldName, int... ids) throws Exception {
        int i = 0;
        for(int val : ids) {
            Tuple t = tuples.get(i);
            Long tip = (Long)t.get(fieldName);
            if(tip.intValue() != val) {
                throw new Exception("Found value:"+tip.intValue()+" expecting:"+val);
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

}
