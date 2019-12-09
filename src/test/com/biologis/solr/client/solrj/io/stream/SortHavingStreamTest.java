package com.biologis.solr.client.solrj.io.stream;

import junit.framework.TestCase;
import org.apache.solr.client.solrj.io.Tuple;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class SortHavingStreamTest extends TestCase {

    private Util util;

    @Override
    protected void setUp() {
        this.util = new Util();
    }

    @Test
    public void test_compareWithOldCall() throws IOException {
        String oldQueryString = "sort(having(search(kmm, fq=\"ss_conjunction:*\", fq=\"ss_direction:*\"),and(eq(ss_conjunction, val(AND)), eq(ss_direction, val(forward)))),by=\"ss_to_link_bag_uuid asc, its_num_links asc\")";
        String newQueryString = "sortHaving(search(kmm, fq=\"ss_conjunction:*\", fq=\"ss_direction:*\"),and(eq(ss_conjunction, val(AND)), eq(ss_direction, val(forward))),by=\"ss_to_link_bag_uuid asc, its_num_links asc\")";

        List<Tuple> oldTuple = this.util.getTuples(oldQueryString);
        List<Tuple> newTuple = this.util.getTuples(newQueryString);

        Assert.assertTrue(oldTuple.size() > 0);
        Assert.assertEquals(oldTuple.size(), newTuple.size());

        for (int i = 0; i < oldTuple.size(); i++) {
            Assert.assertTrue(this.util.areTuplesEqual(oldTuple.get(i), newTuple.get(i)));
        }
    }
}
