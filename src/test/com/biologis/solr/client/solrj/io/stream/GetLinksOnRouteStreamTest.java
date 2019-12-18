package com.biologis.solr.client.solrj.io.stream;

import junit.framework.TestCase;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.InnerJoinStream;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class GetLinksOnRouteStreamTest extends TestCase {

    private Util util;

    @Override
    protected void setUp() {
        this.util = new Util();
    }

    @Ignore
    public void test_compareWithOldCall() throws IOException {
  /*      String oldQueryString = "pepapig(select(innerJoin(unique(sort(having(cartesianProduct(thomasTheTankEngine(reduce(sort(select(innerJoin(select(sort(merge(innerJoin(sort(having(search(kmm,q=*:*,fq=\"+index_id:kmm_graph +hash:tdspmb\",fq=ss_direction:*,fq=its_endpoint_id:(4468),fl=\"its_endpoint_id,ss_endpoint_name,ss_endpoint_type,ss_endpoint_uuid,ss_to_endpoint_uuid,ss_to_link_bag_uuid,ss_to_link_uuid,its_num_links,ss_direction,ss_conjunction,ss_search_api_id_container\",sort=\"ss_to_endpoint_uuid ASC\",qt=\"/export\"),and(eq(ss_conjunction,val(AND)),eq(ss_direction,val(forward)))),by=\"ss_to_link_bag_uuid asc,its_num_links asc\"),sort(rollup(sort(search(kmm,q=*:*,fq=\"+index_id:kmm_graph +hash:tdspmb\",fq=ss_direction:*,fq=its_endpoint_id:(4468),fl=\"its_endpoint_id,ss_endpoint_name,ss_endpoint_type,ss_endpoint_uuid,ss_to_endpoint_uuid,ss_to_link_bag_uuid,ss_to_link_uuid,its_num_links,ss_direction,ss_conjunction,ss_search_api_id_container\",sort=\"ss_to_endpoint_uuid ASC\",qt=\"/export\"),by=\"ss_to_link_bag_uuid asc\"),over=\"ss_to_link_bag_uuid\",count(*)),by=\"ss_to_link_bag_uuid asc,count(*) asc\"),on=\"ss_to_link_bag_uuid,its_num_links=count(*)\"),sort(having(search(kmm,q=*:*,fq=\"+index_id:kmm_graph +hash:tdspmb\",fq=ss_direction:*,fq=its_endpoint_id:(4468),fl=\"its_endpoint_id,ss_endpoint_name,ss_endpoint_type,ss_endpoint_uuid,ss_to_endpoint_uuid,ss_to_link_bag_uuid,ss_to_link_uuid,its_num_links,ss_direction,ss_conjunction,ss_search_api_id_container\",sort=\"ss_to_endpoint_uuid ASC\",qt=\"/export\"),and(eq(ss_conjunction,val(OR)),eq(ss_direction,val(forward)))),by=\"ss_to_link_bag_uuid asc\"),on=\"ss_to_link_bag_uuid asc\"),by=\"ss_to_endpoint_uuid asc\"),its_endpoint_id,ss_endpoint_name,ss_endpoint_type,ss_endpoint_uuid,ss_to_endpoint_uuid,ss_to_link_bag_uuid,ss_to_link_uuid,its_num_links,ss_direction,ss_conjunction,ss_search_api_id_container,concat(fields=\"ss_endpoint_uuid\",delim=\",\",as=\"ancestors\")),unique(search(kmm,q=*:*,fq=ss_endpoint_uuid:d7a8db2f\\-cb33\\-4e85\\-b772\\-85b8b7cae91e,fq=\"+index_id:kmm_graph +hash:tdspmb\",fq=ss_direction:backward,fl=\"ss_endpoint_uuid\",sort=\"ss_endpoint_uuid asc\",qt=\"/export\"),over=ss_endpoint_uuid),on=\"ss_to_endpoint_uuid=ss_endpoint_uuid\"),ss_endpoint_uuid as node,ancestors),by=\"node asc\"),by=\"node\",fieldValueMerge(sort=\"node asc\",mergeField=\"ancestors\",n=\"100000\")),search(kmm,q=*:*,fq=ss_endpoint_uuid:d7a8db2f\\-cb33\\-4e85\\-b772\\-85b8b7cae91e,fq=\"+index_id:kmm_graph +hash:tdspmb\",fq=ss_direction:backward,fl=\"its_endpoint_id,ss_endpoint_name,ss_endpoint_type,ss_endpoint_uuid,ss_to_endpoint_uuid,ss_to_link_bag_uuid,ss_to_link_uuid,its_num_links,ss_direction,ss_conjunction,ss_search_api_id_container\",sort=\"ss_endpoint_uuid asc\",qt=\"/export\"),on=\"node=ss_endpoint_uuid\"),ancestors),eq( ss_to_endpoint_uuid,ancestors)),by=\"ss_to_link_uuid asc\"),over=ss_to_link_uuid),select(search(kmm,q=*:*,fq=\"+index_id:kmm_graph +hash:tdspmb\",fq=ss_link_type:link,fq=ss_search_api_datasource:entity\\:kmm_link,fl=\"its_link_id,ss_link_uuid,ss_search_api_id\",sort=\"ss_link_uuid asc\",qt=\"/export\"),its_link_id,ss_link_uuid,ss_search_api_id),on=\"ss_to_link_uuid=ss_link_uuid\"),its_link_id,ss_link_uuid,ss_search_api_id,ancestors),by=\"ss_link_uuid ASC\")";
        String newQueryString = "getLinksOnRoute(thomasTheTankEngine(reduce(sort(select(innerJoin(select(sort(merge(innerJoin(sort(having(search(kmm,q=*:*,fq=\"+index_id:kmm_graph +hash:tdspmb\",fq=ss_direction:*,fq=its_endpoint_id:(4468),fl=\"its_endpoint_id,ss_endpoint_name,ss_endpoint_type,ss_endpoint_uuid,ss_to_endpoint_uuid,ss_to_link_bag_uuid,ss_to_link_uuid,its_num_links,ss_direction,ss_conjunction,ss_search_api_id_container\",sort=\"ss_to_endpoint_uuid ASC\",qt=\"/export\"),and(eq(ss_conjunction,val(AND)),eq(ss_direction,val(forward)))),by=\"ss_to_link_bag_uuid asc,its_num_links asc\"),sort(rollup(sort(search(kmm,q=*:*,fq=\"+index_id:kmm_graph +hash:tdspmb\",fq=ss_direction:*,fq=its_endpoint_id:(4468),fl=\"its_endpoint_id,ss_endpoint_name,ss_endpoint_type,ss_endpoint_uuid,ss_to_endpoint_uuid,ss_to_link_bag_uuid,ss_to_link_uuid,its_num_links,ss_direction,ss_conjunction,ss_search_api_id_container\",sort=\"ss_to_endpoint_uuid ASC\",qt=\"/export\"),by=\"ss_to_link_bag_uuid asc\"),over=\"ss_to_link_bag_uuid\",count(*)),by=\"ss_to_link_bag_uuid asc,count(*) asc\"),on=\"ss_to_link_bag_uuid,its_num_links=count(*)\"),sort(having(search(kmm,q=*:*,fq=\"+index_id:kmm_graph +hash:tdspmb\",fq=ss_direction:*,fq=its_endpoint_id:(4468),fl=\"its_endpoint_id,ss_endpoint_name,ss_endpoint_type,ss_endpoint_uuid,ss_to_endpoint_uuid,ss_to_link_bag_uuid,ss_to_link_uuid,its_num_links,ss_direction,ss_conjunction,ss_search_api_id_container\",sort=\"ss_to_endpoint_uuid ASC\",qt=\"/export\"),and(eq(ss_conjunction,val(OR)),eq(ss_direction,val(forward)))),by=\"ss_to_link_bag_uuid asc\"),on=\"ss_to_link_bag_uuid asc\"),by=\"ss_to_endpoint_uuid asc\"),its_endpoint_id,ss_endpoint_name,ss_endpoint_type,ss_endpoint_uuid,ss_to_endpoint_uuid,ss_to_link_bag_uuid,ss_to_link_uuid,its_num_links,ss_direction,ss_conjunction,ss_search_api_id_container,concat(fields=\"ss_endpoint_uuid\",delim=\",\",as=\"ancestors\")),unique(search(kmm,q=*:*,fq=ss_endpoint_uuid:d7a8db2f\\-cb33\\-4e85\\-b772\\-85b8b7cae91e,fq=\"+index_id:kmm_graph +hash:tdspmb\",fq=ss_direction:backward,fl=\"ss_endpoint_uuid\",sort=\"ss_endpoint_uuid asc\",qt=\"/export\"),over=ss_endpoint_uuid),on=\"ss_to_endpoint_uuid=ss_endpoint_uuid\"),ss_endpoint_uuid as node,ancestors),by=\"node asc\"),by=\"node\",fieldValueMerge(sort=\"node asc\",mergeField=\"ancestors\",n=\"100000\")),search(kmm,q=*:*,fq=ss_endpoint_uuid:d7a8db2f\\-cb33\\-4e85\\-b772\\-85b8b7cae91e,fq=\"+index_id:kmm_graph +hash:tdspmb\",fq=ss_direction:backward,fl=\"its_endpoint_id,ss_endpoint_name,ss_endpoint_type,ss_endpoint_uuid,ss_to_endpoint_uuid,ss_to_link_bag_uuid,ss_to_link_uuid,its_num_links,ss_direction,ss_conjunction,ss_search_api_id_container\",sort=\"ss_endpoint_uuid asc\",qt=\"/export\"),on=\"node=ss_endpoint_uuid\"))";

        List<Tuple> oldTuples = this.util.getTuples(oldQueryString);
        List<Tuple> newTuples = this.util.getTuples(newQueryString);

        Assert.assertTrue(oldTuples.size() > 0);

        Assert.assertEquals(oldTuples.size(), newTuples.size());

        for (int i = 0; i < oldTuples.size(); i++) {
            Assert.assertTrue(this.util.areTuplesEqual(oldTuples.get(i), newTuples.get(i)));
        }
*/
    }

}
