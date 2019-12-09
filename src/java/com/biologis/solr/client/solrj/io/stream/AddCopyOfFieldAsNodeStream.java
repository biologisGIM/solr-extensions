package com.biologis.solr.client.solrj.io.stream;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.ops.ConcatOperation;
import org.apache.solr.client.solrj.io.stream.SelectStream;
import org.apache.solr.client.solrj.io.stream.StreamContext;
import org.apache.solr.client.solrj.io.stream.TupleStream;
import org.apache.solr.client.solrj.io.stream.expr.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class AddCopyOfFieldAsNodeStream extends TupleStream implements Expressible {

    private TupleStream stream;
    private TupleStream resultStream;
    private List<String> options;
    private ConcatOperation concat;

    public AddCopyOfFieldAsNodeStream(TupleStream stream, String fieldName) throws IOException{
        this.init(stream, fieldName);
    }

    public AddCopyOfFieldAsNodeStream(StreamExpression expression, StreamFactory factory) throws IOException {
        List<StreamExpression> streamExpressions = factory.getExpressionOperandsRepresentingTypes(expression, new Class[]{Expressible.class, TupleStream.class});
        //TODO add checks

        this.init(factory.constructStream((StreamExpression)streamExpressions.get(0)), factory.getValueOperand(expression, 1));
    }

    private void init(TupleStream stream, String fieldName) throws IOException{
        this.stream = stream;
        this.concat = new ConcatOperation(new String[]{fieldName}, "node", ",");
        this.options = this.getSelectParameters();

        SelectStream resultStream = new SelectStream(this.stream, this.options);
        this.resultStream = resultStream;
    }

    private List<String> getSelectParameters(){
        List<String> params = new ArrayList<String>();
        params.add("boost_document");
        params.add("ss_conjunction");
        params.add("its_endpoint_id");
        params.add("ss_endpoint_name");
        params.add("ss_endpoint_type");
        params.add("ss_endpoint_uuid");
        params.add("its_evaluation_id");
        params.add("ss_evaluation_name");
        params.add("ss_evaluation_of_uuid");
        params.add("ss_evaluation_uuid");
        params.add("its_link_id");
        params.add("ss_link_type");
        params.add("ss_link_uuid");
        params.add("ss_metadata_flags");
        params.add("its_metadata_id");
        params.add("ss_metadata_name");
        params.add("ss_metadata_name_md5");
        params.add("ss_metadata_of_uuid");
        params.add("ss_metadata_type");
        params.add("ss_metadata_uuid");
        params.add("bs_status");
        params.add("ss_to_endpoint_uuid");
        params.add("ss_to_link_bag_uuid");
        params.add("ss_search_api_id");
        params.add("ss_search_api_datasource");
        params.add("ss_search_api_language");
        params.add("ss_direction");
        params.add("ss_to_link_uuid");
        params.add("its_num_links");
        params.add("ss_search_api_id_container");
        params.add("id");
        params.add("index_id");
        params.add("hash");
        params.add("site");
        params.add("timestamp");
        params.add("sm_context_tags");
        params.add("spell");
        params.add("sort_X3b_und_search_api_relevance");
        params.add("sort_X3b_und_conjunction");
        params.add("sort_X3b_und_endpoint_name");
        params.add("sort_X3b_und_endpoint_type");
        params.add("sort_X3b_und_endpoint_uuid");
        params.add("sort_X3b_und_evaluation_name");
        params.add("sort_X3b_und_evaluation_of_uuid");
        params.add("sort_X3b_und_evaluation_uuid");
        params.add("sort_X3b_und_link_type");
        params.add("sort_X3b_und_link_uuid");
        params.add("sort_X3b_und_metadata_flags");
        params.add("sort_X3b_und_metadata_name");
        params.add("sort_X3b_und_metadata_name_md5");
        params.add("sort_X3b_und_metadata_of_uuid");
        params.add("sort_X3b_und_metadata_type");
        params.add("sort_X3b_und_metadata_uuid");
        params.add("sort_X3b_und_to_endpoint_uuid");
        params.add("sort_X3b_und_to_link_bag_uuid");
        params.add("sort_X3b_und_search_api_id");
        params.add("sort_X3b_und_search_api_datasource");
        params.add("sort_X3b_und_search_api_language");
        params.add("sort_X3b_und_direction");
        params.add("sort_X3b_und_to_link_uuid");
        params.add("sort_X3b_und_search_api_id_container");
        params.add("sort_X3b_und_site");
        params.add("sort_X3b_und_timestamp");
        params.add("sort_X3b_und_context_tags");
        params.add("sort_X3b_und_spell");
        return params;
    }

    @Override
    public void setStreamContext(StreamContext streamContext) {
        this.resultStream.setStreamContext(streamContext);
    }

    @Override
    public List<TupleStream> children() {
        return this.resultStream.children();
    }

    @Override
    public void open() throws IOException {
        this.resultStream.open();
    }

    @Override
    public void close() throws IOException {
        this.resultStream.close();
    }

    @Override
    public Tuple read() throws IOException {
        Tuple readTuple = this.resultStream.read();
        concat.operate(readTuple);
        return readTuple;
    }

    @Override
    public StreamComparator getStreamSort() {
        return this.resultStream.getStreamSort();
    }

    @Override
    public StreamExpressionParameter toExpression(StreamFactory streamFactory) throws IOException {
        return null;
    }

    @Override
    public Explanation toExplanation(StreamFactory streamFactory) throws IOException {
        return this.resultStream.toExplanation(streamFactory);
    }
}
