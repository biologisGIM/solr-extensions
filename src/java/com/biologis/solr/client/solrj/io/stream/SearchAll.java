package com.biologis.solr.client.solrj.io.stream;

/**
 * return all rows from the collection.
 * <p>
 * Example:
 * searchAll(collectionName, q="*:*")
 * <p>
 */

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.io.SolrClientCache;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.ComparatorOrder;
import org.apache.solr.client.solrj.io.comp.FieldComparator;
import org.apache.solr.client.solrj.io.comp.MultipleFieldComparator;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.stream.StreamContext;
import org.apache.solr.client.solrj.io.stream.TupleStream;
import org.apache.solr.client.solrj.io.stream.expr.*;
import org.apache.solr.client.solrj.io.stream.expr.Explanation.ExpressionType;
import org.apache.solr.client.solrj.request.LukeRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.LukeResponse;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;


public class SearchAll extends TupleStream implements Expressible  {

    private static final long serialVersionUID = 1;

    private String zkHost;
    private ModifiableSolrParams params;
    private String collection;
    protected transient SolrClientCache cache;
    protected transient CloudSolrClient cloudSolrClient;
    private Iterator<SolrDocument> documentIterator;
    protected StreamComparator comp;
    private static String numRows;

    public SearchAll(String zkHost, String collection, ModifiableSolrParams params) {
        init(zkHost, collection, params);
    }

    private void init(String zkHost, String collection, ModifiableSolrParams params) {
        this.zkHost = zkHost;
        this.collection = collection;
        this.params = params;
    }

    public SearchAll(StreamExpression expression, StreamFactory factory) throws IOException{
        // grab all parameters out
        String collectionName = factory.getValueOperand(expression, 0);
        List<StreamExpressionNamedParameter> namedParams = factory.getNamedOperands(expression);
        StreamExpressionNamedParameter zkHostExpression = factory.getNamedOperand(expression, "zkHost");


        // Collection Name
        if(null == collectionName){
            throw new IOException(String.format(Locale.ROOT,"invalid expression %s - collectionName expected as first operand",expression));
        }

        // Named parameters - passed directly to solr as solrparams
        if(0 == namedParams.size()){
            throw new IOException(String.format(Locale.ROOT,"invalid expression %s - at least one named parameter expected. eg. 'q=*:*'",expression));
        }

        // pull out known named params
        ModifiableSolrParams params = new ModifiableSolrParams();
        for(StreamExpressionNamedParameter namedParam : namedParams){
            if(!namedParam.getName().equals("zkHost") && !namedParam.getName().equals("buckets") && !namedParam.getName().equals("bucketSorts") && !namedParam.getName().equals("limit")){
                params.add(namedParam.getName(), namedParam.getParameter().toString().trim());
            }
        }

        // zkHost, optional - if not provided then will look into factory list to get
        String zkHost = null;
        if(null == zkHostExpression){
            zkHost = factory.getCollectionZkHost(collectionName);
            if(zkHost == null) {
                zkHost = factory.getDefaultZkHost();
            }
        }
        else if(zkHostExpression.getParameter() instanceof StreamExpressionValue){
            zkHost = ((StreamExpressionValue)zkHostExpression.getParameter()).getValue();
        }
        if(null == zkHost){
            throw new IOException(String.format(Locale.ROOT,"invalid expression %s - zkHost not found for collection '%s'",expression,collectionName));
        }

        // We've got all the required items
        init(zkHost, collectionName, params);

        if(this.params.get(CommonParams.Q) == null) {
            this.params.add(CommonParams.Q, "*:*");
        }

        if(this.params.get(CommonParams.ROWS) == null) {
            this.params.add(CommonParams.ROWS, numRows);
        }

        if(params.get(CommonParams.SORT) != null) {
            this.comp = parseComp(params.get(CommonParams.SORT), params.get(CommonParams.FL));
        }

        // Gather all the required items
        init(zkHost, collectionName, params);
    }

    @Override
    public StreamExpressionParameter toExpression(StreamFactory factory) throws IOException {
        // function name
        StreamExpression expression = new StreamExpression(factory.getFunctionName((this.getClass())));

        // collection
        if(collection.indexOf(',') > -1) {
            expression.addParameter("\""+collection+"\"");
        } else {
            expression.addParameter(collection);
        }

        for (Entry<String, String[]> param : params.getMap().entrySet()) {
            for (String val : param.getValue()) {
                expression.addParameter(new StreamExpressionNamedParameter(param.getKey(),
                        val.replace("\"", "\\\"")));
            }
        }

        // zkHost
        expression.addParameter(new StreamExpressionNamedParameter("zkHost", zkHost));

        return expression;
    }

    @Override
    public Explanation toExplanation(StreamFactory factory) throws IOException {

        StreamExplanation explanation = new StreamExplanation(getStreamNodeId().toString());

        explanation.setFunctionName(factory.getFunctionName((this.getClass())));
        explanation.setImplementingClass(this.getClass().getName());
        explanation.setExpressionType(ExpressionType.STREAM_SOURCE);
        explanation.setExpression(toExpression(factory).toString());


        StreamExplanation child = new StreamExplanation(getStreamNodeId() + "-datastore");
        child.setFunctionName(String.format(Locale.ROOT, "solr (%s)", collection));
        child.setImplementingClass("Solr/Lucene");
        child.setExpressionType(ExpressionType.DATASTORE);

        explanation.addChild(child);

        return explanation;
    }

    public void setStreamContext(StreamContext context) {
        cache = context.getSolrClientCache();
    }

    public List<TupleStream> children() {
        List<TupleStream> l =  new ArrayList();
        return l;
    }

    public void open() throws IOException {
        if(cache != null) {
            cloudSolrClient = cache.getCloudSolrClient(zkHost);
        } else {
            final List<String> hosts = new ArrayList<>();
            hosts.add(zkHost);
            cloudSolrClient = new CloudSolrClient.Builder(hosts, Optional.empty()).build();
        }


        QueryRequest request = new QueryRequest(params,  SolrRequest.METHOD.POST);
        try {
            QueryResponse response = request.process(cloudSolrClient, collection);
            SolrDocumentList docs = response.getResults();
            documentIterator = docs.iterator();
        } catch (Exception e) {
            throw new IOException(e);
        }

        LukeRequest req = new LukeRequest();
        try {
            LukeResponse resp = req.process(cloudSolrClient, collection);
            Integer nDocs = resp.getNumDocs();
            numRows = Integer.toString(nDocs);
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    public void close() throws IOException {
        if(cache == null) {
            cloudSolrClient.close();
        }
    }

    public Tuple read() throws IOException {
        if(documentIterator.hasNext()) {
            Map map = new HashMap();
            SolrDocument doc = documentIterator.next();
            for(String key  : doc.keySet()) {
                map.put(key, doc.get(key));
            }
            return new Tuple(map);
        } else {
            Map fields = new HashMap();
            fields.put("EOF", true);
            Tuple tuple = new Tuple(fields);
            return tuple;
        }
    }


    public int getCost() {
        return 0;
    }

    @Override
    public StreamComparator getStreamSort() {
        return comp;
    }

    private StreamComparator parseComp(String sort, String fl) throws IOException {

        HashSet fieldSet = null;

        if(fl != null) {
            fieldSet = new HashSet();
            String[] fls = fl.split(",");
            for (String f : fls) {
                fieldSet.add(f.trim()); //Handle spaces in the field list.
            }
        }

        String[] sorts = sort.split(",");
        StreamComparator[] comps = new StreamComparator[sorts.length];
        for(int i=0; i<sorts.length; i++) {
            String s = sorts[i];

            String[] spec = s.trim().split("\\s+");

            if (spec.length != 2) {
                throw new IOException("Invalid sort spec:" + s);
            }

            String fieldName = spec[0].trim();
            String order = spec[1].trim();

            if(fieldSet != null && !fieldSet.contains(spec[0])) {
                throw new IOException("Fields in the sort spec must be included in the field list:"+spec[0]);
            }

            comps[i] = new FieldComparator(fieldName, order.equalsIgnoreCase("asc") ? ComparatorOrder.ASCENDING : ComparatorOrder.DESCENDING);
        }

        if(comps.length > 1) {
            return new MultipleFieldComparator(comps);
        } else {
            return comps[0];
        }
    }
}
