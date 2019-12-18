package com.biologis.solr.client.solrj.io.stream;

import junit.framework.TestCase;
import org.apache.solr.client.solrj.io.Tuple;
import org.junit.*;

import java.io.IOException;
import java.util.List;

public class AddCopyOfFieldAsNodeStreamTest extends TestCase {

    private Util util;

    @Override
    protected void setUp(){
        this.util = new Util();
    }

    @Ignore
    public void test_compareWithOldCall() throws IOException{
/*        String oldQueryString = "select(search(  kmm,  q=*:*,  fq=\"+index_id:kmm_graph +hash:tdspmb\",  fq=ss_direction:*,  fq=ss_endpoint_type:gene,  fl=\"its_endpoint_id,  ss_endpoint_name,  ss_endpoint_type,  ss_endpoint_uuid,  ss_to_endpoint_uuid,  ss_to_link_bag_uuid,  ss_to_link_uuid,  its_num_links,  ss_direction,  ss_conjunction,  ss_search_api_id_container\",  sort=\"ss_to_endpoint_uuid ASC\",  qt=\"/export\"),boost_document,ss_conjunction,its_endpoint_id,ss_endpoint_name,ss_endpoint_type,ss_endpoint_uuid,its_evaluation_id,ss_evaluation_name,ss_evaluation_of_uuid,ss_evaluation_uuid,its_link_id,ss_link_type,ss_link_uuid,ss_metadata_flags,its_metadata_id,ss_metadata_name,ss_metadata_name_md5,ss_metadata_of_uuid,ss_metadata_type,ss_metadata_uuid,bs_status,ss_to_endpoint_uuid,ss_to_link_bag_uuid,ss_search_api_id,ss_search_api_datasource,ss_search_api_language,ss_direction,ss_to_link_uuid,its_num_links,ss_search_api_id_container,id,index_id,hash,site,timestamp,sm_context_tags,spell,sort_X3b_und_search_api_relevance,sort_X3b_und_conjunction,sort_X3b_und_endpoint_name,sort_X3b_und_endpoint_type,sort_X3b_und_endpoint_uuid,sort_X3b_und_evaluation_name,sort_X3b_und_evaluation_of_uuid,sort_X3b_und_evaluation_uuid,sort_X3b_und_link_type,sort_X3b_und_link_uuid,sort_X3b_und_metadata_flags,sort_X3b_und_metadata_name,sort_X3b_und_metadata_name_md5,sort_X3b_und_metadata_of_uuid,sort_X3b_und_metadata_type,sort_X3b_und_metadata_uuid,sort_X3b_und_to_endpoint_uuid,sort_X3b_und_to_link_bag_uuid,sort_X3b_und_search_api_id,sort_X3b_und_search_api_datasource,sort_X3b_und_search_api_language,sort_X3b_und_direction,sort_X3b_und_to_link_uuid,sort_X3b_und_search_api_id_container,sort_X3b_und_site,sort_X3b_und_timestamp,sort_X3b_und_context_tags,sort_X3b_und_spell,concat(  fields=\"ss_endpoint_uuid\",  delim=\", \",  as=\"node\"))";
        String newQueryString = "addCopyOfFieldAsNode(search(kmm,q=*:*,fq=\"+index_id:kmm_graph +hash:tdspmb\",fq=ss_direction:*,fq=ss_endpoint_type:gene,fl=\"its_endpoint_id,ss_endpoint_name,ss_endpoint_type,ss_endpoint_uuid,ss_to_endpoint_uuid,ss_to_link_bag_uuid,ss_to_link_uuid,its_num_links,ss_direction,ss_conjunction,ss_search_api_id_container\",sort=\"ss_to_endpoint_uuid ASC\",qt=\"/export\"),ss_endpoint_uuid)";

        List<Tuple> oldTuples = this.util.getTuples(oldQueryString);
        List<Tuple> newTuples = this.util.getTuples(newQueryString);

        Assert.assertEquals(oldTuples.size(), newTuples.size());

        for(int i = 0; i < oldTuples.size(); i++){
            Assert.assertTrue(this.util.areTuplesEqual(oldTuples.get(i), newTuples.get(i)));
        }
*/
    }


}