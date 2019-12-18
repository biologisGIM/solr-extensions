package com.biologis.solr.client.solrj.io.util;

import com.biologis.solr.client.solrj.io.stream.GetTargetNodesStream;
import com.biologis.solr.client.solrj.io.stream.Util;
import com.biologis.solr.client.solrj.io.util.BiologisCache;
import junit.framework.TestCase;
import org.apache.solr.client.solrj.io.eval.EqualToEvaluator;
import org.apache.solr.client.solrj.io.ops.ConcatOperation;
import org.apache.solr.client.solrj.io.ops.FieldValueMergeOperation;
import org.apache.solr.client.solrj.io.stream.*;
import org.apache.solr.client.solrj.io.stream.expr.Expressible;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.apache.solr.search.CacheConfig;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.mockito.Mockito.*;

public class CacheManagerTest extends TestCase {

    private CacheManager cacheManager;

    @Override
    public void setUp() throws IOException {

        CacheFactory cacheFactory = spy(CacheFactory.class);

        when(cacheFactory.getCache(Mockito.any())).thenReturn(mockCache());

        this.cacheManager = new CacheManager(cacheFactory);
    }

    private BiologisCache mockCache(){
        Map<String,String> args = new LinkedHashMap<String, String>();
        args.put("autowarmCount", "1024");
        args.put("size", "4096");
        args.put("initialSize", "1024");
        args.put("name", "biologisClass");
        args.put("class", "com.biologis.solr.client.solrj.io.util.BiologisCache");
        CacheConfig config = new CacheConfig(BiologisCache.class, args, null);
    }

    @Test
    public void test() throws IOException{
        StreamFactory sf = new StreamFactory();
        sf.withFunctionName("getTargetNodes", GetTargetNodesStream.class);
        sf.withFunctionName("innerJoin", InnerJoinStream.class);
        sf.withFunctionName("reduce", ReducerStream.class);
        sf.withFunctionName("sort", SortStream.class);
        sf.withFunctionName("select", SelectStream.class);
        sf.withFunctionName("having", HavingStream.class);
        sf.withFunctionName("search", SearchFacadeStream.class);
        sf.withFunctionName("fieldValueMerge", FieldValueMergeOperation.class);
        sf.withFunctionName("unique", UniqueStream.class);
        sf.withFunctionName("concat", ConcatOperation.class);
        sf.withFunctionName("eq", EqualToEvaluator.class);
        sf.withDefaultZkHost("zoo01:2181");
        TupleStream exp = sf.constructStream("getTargetNodes(innerJoin(reduce(sort(select(innerJoin(select(sort(having(select(search(kmm,q=*:*,fq=\"+index_id:kmm_graph +hash:tdspmb\",fq=ss_direction:*,fq=ss_endpoint_type:gene,fl=\"its_endpoint_id,ss_endpoint_name,ss_endpoint_type,ss_endpoint_uuid,ss_to_endpoint_uuid,ss_to_link_bag_uuid,ss_to_link_uuid,its_num_links,ss_direction,ss_conjunction,ss_search_api_id_container\",sort=\"ss_to_endpoint_uuid ASC\",qt=\"/export\"),boost_document,ss_conjunction,its_endpoint_id,ss_endpoint_name,ss_endpoint_type,ss_endpoint_uuid,its_evaluation_id,ss_evaluation_name,ss_evaluation_of_uuid,ss_evaluation_uuid,its_link_id,ss_link_type,ss_link_uuid,ss_metadata_flags,its_metadata_id,ss_metadata_name,ss_metadata_name_md5,ss_metadata_of_uuid,ss_metadata_type,ss_metadata_uuid,bs_status,ss_to_endpoint_uuid,ss_to_link_bag_uuid,ss_search_api_id,ss_search_api_datasource,ss_search_api_language,ss_direction,ss_to_link_uuid,its_num_links,ss_search_api_id_container,id,index_id,hash,site,timestamp,sm_context_tags,spell,sort_X3b_und_search_api_relevance,sort_X3b_und_conjunction,sort_X3b_und_endpoint_name,sort_X3b_und_endpoint_type,sort_X3b_und_endpoint_uuid,sort_X3b_und_evaluation_name,sort_X3b_und_evaluation_of_uuid,sort_X3b_und_evaluation_uuid,sort_X3b_und_link_type,sort_X3b_und_link_uuid,sort_X3b_und_metadata_flags,sort_X3b_und_metadata_name,sort_X3b_und_metadata_name_md5,sort_X3b_und_metadata_of_uuid,sort_X3b_und_metadata_type,sort_X3b_und_metadata_uuid,sort_X3b_und_to_endpoint_uuid,sort_X3b_und_to_link_bag_uuid,sort_X3b_und_search_api_id,sort_X3b_und_search_api_datasource,sort_X3b_und_search_api_language,sort_X3b_und_direction,sort_X3b_und_to_link_uuid,sort_X3b_und_search_api_id_container,sort_X3b_und_site,sort_X3b_und_timestamp,sort_X3b_und_context_tags,sort_X3b_und_spell,concat(fields=\"ss_endpoint_uuid\",delim=\",\",as=\"node\")),eq(ss_direction,val(backward))),by=\"ss_to_endpoint_uuid asc\"),its_endpoint_id,ss_endpoint_name,ss_endpoint_type,ss_endpoint_uuid,ss_to_endpoint_uuid,ss_to_link_bag_uuid,ss_to_link_uuid,its_num_links,ss_direction,ss_conjunction,ss_search_api_id_container,concat(fields=\"ss_endpoint_uuid\",delim=\",\",as=\"ancestors\")),unique(search(kmm,q=*:*,fq=ss_endpoint_type:position,fq=\"+index_id:kmm_graph +hash:tdspmb\",fq=ss_direction:forward,fl=\"ss_endpoint_uuid\",sort=\"ss_endpoint_uuid asc\",qt=\"/export\"),over=ss_endpoint_uuid),on=\"ss_to_endpoint_uuid=ss_endpoint_uuid\"),ss_endpoint_uuid as node,ancestors),by=\"node asc\"),by=\"node\",fieldValueMerge(sort=\"node asc\",mergeField=\"ancestors\",n=\"100000\")),search(kmm,q=*:*,fq=ss_endpoint_type:position,fq=\"+index_id:kmm_graph +hash:tdspmb\",fq=ss_direction:forward,fl=\"its_endpoint_id,ss_endpoint_name,ss_endpoint_type,ss_endpoint_uuid,ss_to_endpoint_uuid,ss_to_link_bag_uuid,ss_to_link_uuid,its_num_links,ss_direction,ss_conjunction,ss_search_api_id_container\",sort=\"ss_endpoint_uuid asc\",qt=\"/export\"),on=\"node=ss_endpoint_uuid\"))");


        Expressible expression = (Expressible) exp;

        Object cacheHit = cacheManager.getCashedStream(expression, sf, null);

        assertNull(cacheHit);

        cacheHit = cacheManager.putInCache(expression, sf, null);

        assertNotNull(cacheHit);
    }


}
