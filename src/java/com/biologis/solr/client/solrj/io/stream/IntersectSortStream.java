package com.biologis.solr.client.solrj.io.stream;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.FieldComparator;
import org.apache.solr.client.solrj.io.comp.MultipleFieldComparator;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.eq.FieldEqualitor;
import org.apache.solr.client.solrj.io.eq.MultipleFieldEqualitor;
import org.apache.solr.client.solrj.io.eq.StreamEqualitor;
import org.apache.solr.client.solrj.io.stream.*;
import org.apache.solr.client.solrj.io.stream.expr.*;

import java.io.IOException;
import java.util.List;
import java.util.Locale;

/*
IntersectSortStream

Streaming expression that received two streams and a field with field order and generates the intersection of them based on the provided field
Equivalent to
intersect(
      sort(
        $stream1,
        'by="' . $solr_field . ' ASC"'
      ),
      sort(
        $stream2,
        'by="' . $solr_field . ' ASC"'
      ),
      'on=' . $solr_field
);
 */
public class IntersectSortStream extends TupleStream implements Expressible {

    private TupleStream streamA;
    private TupleStream streamB;
    private SortStream sortStreamA;
    private SortStream sortStreamB;
    private StreamComparator cp;
    private IntersectStream intersectStream;

    public IntersectSortStream(TupleStream streamA, TupleStream streamB, StreamComparator cp) throws IOException{
        this.init(streamA, streamB, cp);
    }

    public IntersectSortStream(StreamExpression expression, StreamFactory factory) throws IOException {
        List<StreamExpression> streamExpressions = factory.getExpressionOperandsRepresentingTypes(expression, new Class[]{Expressible.class, TupleStream.class});
        StreamExpressionNamedParameter onExpression = factory.getNamedOperand(expression, "on");

        if (expression.getParameters().size() != streamExpressions.size() + 1) {
            throw new IOException(String.format(Locale.ROOT, "Invalid expression %s - unknown operands found", expression));

        } else if (streamExpressions.size() != 2) {
            throw new IOException(String.format(Locale.ROOT, "Invalid expression %s - expecting two streams but found %d (must be TupleStream types)", expression, streamExpressions.size()));

        } else if (onExpression != null && onExpression.getParameter() instanceof StreamExpressionValue) {
            this.init(
                    factory.constructStream((StreamExpression)streamExpressions.get(0)),
                    factory.constructStream((StreamExpression)streamExpressions.get(1)),
                    factory.constructComparator(((StreamExpressionValue)onExpression.getParameter()).getValue(), FieldComparator.class));

        } else {
            throw new IOException(String.format(Locale.ROOT, "Invalid expression %s - expecting single 'on' parameter listing fields to merge on but didn't find one", expression));
        }
    }

    private void init(TupleStream streamA, TupleStream streamB, StreamComparator cp) throws IOException{
        this.streamA = streamA;
        this.streamB = streamB;
        this.cp = cp;
    }

    @Override
    public void setStreamContext(StreamContext streamContext) {
        this.streamA.setStreamContext(streamContext);
        this.streamB.setStreamContext(streamContext);
    }

    @Override
    public List<TupleStream> children() {
        return this.intersectStream.children();
    }

    @Override
    public void open() throws IOException {
        //Sort A
        this.sortStreamA = new SortStream(this.streamA, this.cp);
        this.sortStreamA.open();
        //Sort B
        this.sortStreamB = new SortStream(this.streamB, this.cp);
        this.sortStreamB.open();

        // Intersect sorted A with sorted B
        IntersectStream intersectStream = new IntersectStream(this.sortStreamA, this.sortStreamB, this.convertToEqualitor(cp));
        this.intersectStream = intersectStream;
        this.intersectStream.open();
    }

    private StreamEqualitor convertToEqualitor(StreamComparator comp) {
        if (!(comp instanceof MultipleFieldComparator)) {
            FieldComparator fComp = (FieldComparator)comp;
            return new FieldEqualitor(fComp.getLeftFieldName(), fComp.getRightFieldName());
        } else {
            MultipleFieldComparator mComp = (MultipleFieldComparator)comp;
            StreamEqualitor[] eqs = new StreamEqualitor[mComp.getComps().length];

            for(int idx = 0; idx < mComp.getComps().length; ++idx) {
                eqs[idx] = this.convertToEqualitor(mComp.getComps()[idx]);
            }

            return new MultipleFieldEqualitor(eqs);
        }
    }

    @Override
    public void close() throws IOException {
        this.sortStreamA.close();
        this.sortStreamB.close();
        this.intersectStream.close();
    }

    @Override
    public Tuple read() throws IOException {
        return this.intersectStream.read();
    }

    @Override
    public StreamComparator getStreamSort() {
        return this.intersectStream.getStreamSort();
    }

    @Override
    public StreamExpressionParameter toExpression(StreamFactory streamFactory) throws IOException {
        return this.intersectStream.toExpression(streamFactory);
    }

    @Override
    public Explanation toExplanation(StreamFactory streamFactory) throws IOException {
        return this.intersectStream.toExplanation(streamFactory);
    }
}
