package com.biologis.solr.client.solrj.io.stream;

import com.biologis.solr.client.solrj.io.util.HavingUniqueWorker;
import com.biologis.solr.client.solrj.io.util.TupleContainer;
import com.biologis.solr.client.solrj.io.util.Worker;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.FieldComparator;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.eq.FieldEqualitor;
import org.apache.solr.client.solrj.io.stream.*;
import org.apache.solr.client.solrj.io.stream.expr.*;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;

/**
 * Equivalent of
 *       $this->sort(
 *         $this->select(
 *           $this->innerJoin(
 *             $this->unique(
 *               $this->sort(
 *                 $this->having(
 *                   $this->cartesianProduct(
 *                     $stream,
 *                     'ancestors'
 *                   ),
 *                   'eq( ' . $this->_field('to_endpoint_uuid') . ', ancestors)'
 *                  ),
 *                 'by="' . $this->_field('to_link_uuid') . ' asc"'
 *               ),
 *               'over='. $this->_field('to_link_uuid')
 *             ),
 *             $this->select(
 *               $this->_export_all(
 *                 'q=*:*',
 *                 'fq="' . $this->_index_filter_query() . '"',
 *                 'fq=' . $this->_field_escaped_value('link_type', 'link'),
 *                 'fq=' . $this->_field_escaped_value('search_api_datasource', 'entity:kmm_link'),
 *                 'fl="' . $this->_field_list([
 *                   'link_id',
 *                   'link_uuid',
 *                   'search_api_id',
 *                 ]) . '"',
 *                 'sort="' . $this->_field('link_uuid') . ' asc"'
 *               ),
 *               $this->_field_list([
 *                 'link_id',
 *                 'link_uuid',
 *                 'search_api_id',
 *               ])
 *             ),
 *             'on="' . $this->_field('to_link_uuid') . '=' . $this->_field('link_uuid') . '"'
 *           ),
 *           $this->_field_list([
 *             'link_id',
 *             'link_uuid',
 *             'search_api_id',
 *           ]),
 *           'ancestors'
 *         ),
 *         'by="' . $this->_field('link_uuid') . ' ASC"'
 *       );
 */
public class GetLinksOnRouteStream extends CachedTupleStream implements Expressible {

    private Worker worker;
    private HashSet<String> options;
    private StreamComparator comparator;

    public GetLinksOnRouteStream(StreamExpression expression, StreamFactory factory) throws IOException {
        List<StreamExpression> streamExpressions = factory.getExpressionOperandsRepresentingTypes(expression, new Class[]{Expressible.class, TupleStream.class});

        if (expression.getParameters().size() != streamExpressions.size()) {
            throw new IOException(String.format(Locale.ROOT, "Invalid expression %s - unknown operands found", expression));

        } else if (streamExpressions.size() != 1) {
            throw new IOException(String.format(Locale.ROOT, "Invalid expression %s - expecting one stream but found %d (must be TupleStream types)", expression, streamExpressions.size()));

        } else {
            this.init(factory.constructStream(streamExpressions.get(0)), factory);
        }
    }

    private void init(TupleStream stream, StreamFactory factory) throws IOException{
        this.stream = stream;
        this.factory = factory;
        this.options = this.getOptions();
        this.comparator = this.factory.constructComparator("ss_link_uuid asc", FieldComparator.class);
    }

    private HashSet<String> getOptions(){
        HashSet<String> options = new HashSet();
        options.add("its_link_id");
        options.add("ss_link_uuid");
        options.add("ss_search_api_id");
        options.add("ancestors");

        return options;
    }

    @Override
    public void setStreamContext(StreamContext streamContext) {
        this.stream.setStreamContext(streamContext);
        this.streamContext = streamContext;
    }

    @Override
    public List<TupleStream> children() {
        return null;
    }

    protected void transform() throws IOException{
        this.stream.open();

        // cartesian product of stream

        // a (Unique Sort Having with cartesian product)
        HavingUniqueWorker havingUniqueWorker = new HavingUniqueWorker("ss_to_link_uuid", "ss_to_endpoint_uuid", "ancestors", this.factory.constructComparator("ss_to_link_uuid asc", FieldComparator.class));
        havingUniqueWorker.readStream(this.stream);
        havingUniqueWorker.sort();

        // b (search)
        TupleStream search = this.factory.constructStream("search(kmm,q=*:*,fq=\"+index_id:kmm_graph +hash:tdspmb\",fq=ss_link_type:link,fq=ss_search_api_datasource:entity\\:kmm_link,fl=\"its_link_id,ss_link_uuid,ss_search_api_id\",sort=\"ss_link_uuid asc\",qt=\"/export\")");
        this.setThisStreamContextAndOpen(search);

        // inner join of a and b
        InnerJoinStream innerJoin = new InnerJoinStream(new TupleContainer(havingUniqueWorker), search, factory.constructEqualitor("ss_to_link_uuid=ss_link_uuid", FieldEqualitor.class));

        // sort inner join
        this.worker = new Worker(this.comparator);
        this.worker.readStream(innerJoin);
        this.worker.sort();

        // close local streams
        search.close();
    }

    private void setThisStreamContextAndOpen(TupleStream stream) throws IOException{
        stream.setStreamContext(this.streamContext);
        stream.open();
    }

    @Override
    public void close() throws IOException {
        this.stream.close();
    }

    public Tuple readNormal() throws IOException{
        Tuple original = this.worker.read();

        if(original.EOF){
            return original;
        }

        Tuple result = new Tuple();

        for(Object field : original.fields.keySet()){
            if(this.options.contains(field)){
                result.put(field, original.get(field));
            }
        }

        return result;
    }

    @Override
    public StreamComparator getStreamSort() {
        return this.comparator;
    }

    @Override
    public StreamExpression toExpression(StreamFactory streamFactory) throws IOException {
        StreamExpression expression = new StreamExpression(factory.getFunctionName(this.getClass()));

        if (stream instanceof Expressible) {
            expression.addParameter(((Expressible) stream).toExpression(factory));
        } else {
            throw new IOException("The EvalStream contains a non-expressible TupleStream - it cannot be converted to an expression");
        }

        return expression;
    }

    @Override
    public Explanation toExplanation(StreamFactory streamFactory) throws IOException {
        return null;
    }

}
