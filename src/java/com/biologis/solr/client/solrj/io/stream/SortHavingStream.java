package com.biologis.solr.client.solrj.io.stream;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.FieldComparator;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.eval.RecursiveBooleanEvaluator;
import org.apache.solr.client.solrj.io.eval.StreamEvaluator;
import org.apache.solr.client.solrj.io.stream.HavingStream;
import org.apache.solr.client.solrj.io.stream.SortStream;
import org.apache.solr.client.solrj.io.stream.StreamContext;
import org.apache.solr.client.solrj.io.stream.TupleStream;
import org.apache.solr.client.solrj.io.stream.expr.*;
import org.apache.solr.client.solrj.io.stream.expr.Explanation.ExpressionType;

/*
 StreamingExpression equivalent to:

sort(
        $this->having(
        $stream,
        'and(eq(' . $this->_field(conj) . ', val(and)),eq(' . $this->_field(direkt) . ', val(fwd)))'
        ),
        'by="' . $this->_field('lbuuid') . ' asc,' . $this->_field('nl') . ' asc"'
        )
*/

public class SortHavingStream extends TupleStream implements Expressible {

    private static final long serialVersionUID = 1;

    private TupleStream stream;
    private HavingStream havingStream;
    private StreamComparator comp;
    private SortStream sortStream;
    private RecursiveBooleanEvaluator eval;

    public SortHavingStream(TupleStream stream, RecursiveBooleanEvaluator eval, StreamComparator comp) throws IOException {
        this.init(stream, eval, comp);
    }

    public SortHavingStream(StreamExpression expression, StreamFactory factory) throws IOException {
        List<StreamExpression> streamExpressions = factory.getExpressionOperandsRepresentingTypes(expression, Expressible.class, TupleStream.class);
        List<StreamExpression> evaluatorExpressions = factory.getExpressionOperandsRepresentingTypes(expression, RecursiveBooleanEvaluator.class);
        StreamExpressionNamedParameter byExpression = factory.getNamedOperand(expression, "by");

        if (expression.getParameters().size() != streamExpressions.size() + 1) {
            throw new IOException(String.format(Locale.ROOT, "Invalid expression %s - unknown operands found", expression));
        }

        if (streamExpressions.size() != 1) {
            throw new IOException(String.format(Locale.ROOT, "Invalid expression %s - expecting a single stream but found %d", expression, streamExpressions.size()));
        }

        if (byExpression == null || !(byExpression.getParameter() instanceof StreamExpressionValue)) {
            throw new IOException(String.format(Locale.ROOT, "Invalid expression %s - expecting single 'by' parameter listing fields to sort over but didn't find one", expression));
        }

        StreamEvaluator eval = null;
        if(evaluatorExpressions != null && evaluatorExpressions.size() == 1) {
            StreamExpression ex = evaluatorExpressions.get(0);
            eval = factory.constructEvaluator(ex);
            if(!(eval instanceof RecursiveBooleanEvaluator)) {
                throw new IOException("The SortHavingStream requires a RecursiveBooleanEvaluator. A StreamEvaluator was provided.");
            }
        } else {
            throw new IOException("The SortHavingStream requires a RecursiveBooleanEvaluator.");
        }

        init(
                factory.constructStream(streamExpressions.get(0)),
                (RecursiveBooleanEvaluator)eval,
                factory.constructComparator(((StreamExpressionValue)byExpression.getParameter()).getValue(), FieldComparator.class));
    }

    private void init(TupleStream stream, RecursiveBooleanEvaluator eval, StreamComparator comp) throws IOException {
        this.stream = stream;
        this.eval = eval;
        this.comp = comp;
    }

    @Override
    public void setStreamContext(StreamContext context) {
        this.stream.setStreamContext(context);
        this.eval.setStreamContext(context);
    }

    @Override
    public List<TupleStream> children() {
        return this.havingStream.children();
    }

    @Override
    public void open() throws IOException {
        // evaluate the stream
        this.havingStream = new HavingStream(this.stream, this.eval);
        this.havingStream.open();

        // sort the stream
        this.sortStream = new SortStream(this.havingStream, this.comp);
        this.sortStream.open();
    }

    @Override
    public void close() throws IOException {
        this.havingStream.close();
        this.sortStream.close();
    }

    @Override
    public Tuple read() throws IOException {
        return this.sortStream.read();
    }

    @Override
    public StreamComparator getStreamSort() {
        return this.sortStream.getStreamSort();
    }

    @Override
    public StreamExpressionParameter toExpression(StreamFactory streamFactory) throws IOException {
        return this.sortStream.toExpression(streamFactory);
    }

    @Override
    public Explanation toExplanation(StreamFactory streamFactory) throws IOException {
        return this.sortStream.toExplanation(streamFactory);
    }
}