package com.biologis.solr.client.solrj.io.stream;

import com.biologis.solr.client.solrj.io.ops.DefinedConstant;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.eval.RecursiveBooleanEvaluator;
import org.apache.solr.client.solrj.io.stream.HavingStream;
import org.apache.solr.client.solrj.io.stream.StreamContext;
import org.apache.solr.client.solrj.io.stream.TupleStream;
import org.apache.solr.client.solrj.io.stream.expr.*;
import org.noggit.JSONParser;
import org.noggit.JSONWriter;
import org.noggit.ObjectBuilder;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class walkToNextNodeLevelStream extends TupleStream implements Expressible {

    private TupleStream stream;
    private String JSONOptions;
    private LinkedHashMap options;

    public walkToNextNodeLevelStream(TupleStream stream, String jsonOptions) throws IOException{
        this.init(stream, jsonOptions);
    }

    public walkToNextNodeLevelStream(StreamExpression expression, StreamFactory factory) throws IOException{
        List<StreamExpression>  streamExpressions = factory.getExpressionOperandsRepresentingTypes(expression, new Class[]{Expressible.class, TupleStream.class});
        StreamExpressionNamedParameter jsonExpression = factory.getNamedOperand(expression, "json");

        if (expression.getParameters().size() != streamExpressions.size() + 1) {
            throw new IOException(String.format(Locale.ROOT, "Invalid expression %s - unknown operands found", expression));

        }
        else if (streamExpressions.size() != 2) {
            throw new IOException(String.format(Locale.ROOT, "Invalid expression %s - expecting two streams but found %d (must be TupleStream types)", expression, streamExpressions.size()));
        }
        else if (jsonExpression != null && jsonExpression.getParameter() instanceof StreamExpressionValue){
            this.init(
                    factory.constructStream(streamExpressions.get(0)),
                    jsonExpression.getParameter().toString().replaceAll("'", ""));
        }else {
            throw new IOException(String.format(Locale.ROOT, "Invalid expression %s - expecting single 'on' parameter listing fields to merge on but didn't find one", expression));
        }
    }

    private void init(TupleStream stream, String jsonOptions) throws IOException{
        this.stream = stream;
        this.JSONOptions = jsonOptions;
        JSONParser parser = new JSONParser(jsonOptions);
        ObjectBuilder ob = new ObjectBuilder(parser);
        this.options = (LinkedHashMap)ob.getObject();
        this.setDefaultOptions();
    }

    private void setDefaultOptions(){
        this.options.putIfAbsent(DefinedConstant.GF_DIRECTION, DefinedConstant.GV_FORWARD);
        this.options.putIfAbsent(DefinedConstant.O_USE_CONJUCTION, true);
        this.options.putIfAbsent(DefinedConstant.O_LAST_WALK, false);

        if (this.options.get(DefinedConstant.GF_DIRECTION).equals(DefinedConstant.GV_FORWARD)){
            this.options.put(DefinedConstant.O_REVERSE_DIRECTION, DefinedConstant.GV_BACKWARD);
        }
        else{
            this.options.put(DefinedConstant.O_REVERSE_DIRECTION, DefinedConstant.GV_FORWARD);
        }
    }


    @Override
    public void setStreamContext(StreamContext streamContext) {
        this.stream.setStreamContext(streamContext);
    }

    @Override
    public List<TupleStream> children() {
        return this.stream.children();
    }

    @Override
    public void open() throws IOException {
        TupleStream filteredStream = this.getFilteredStream();
    }

    private TupleStream getFilteredStream(){
        TupleStream filteredStream;
        if (isDirectionBackward() || useCnjuction() ){
            //RecursiveBooleanEvaluator evaluator = new RecursiveBooleanEvaluator();

            //HavingStream havingStream = new HavingStream(this.stream, )
        }

        return null;
    }

    private boolean isDirectionBackward(){
        return this.options.get(DefinedConstant.GF_DIRECTION).equals(DefinedConstant.GV_BACKWARD);
    }

    private boolean useCnjuction(){
        return (boolean)this.options.get(DefinedConstant.O_USE_CONJUCTION);
    }

    @Override
    public void close() throws IOException {
        this.stream.close();
    }

    @Override
    public Tuple read() throws IOException {
        return null;
    }

    @Override
    public StreamComparator getStreamSort() {
        return this.stream.getStreamSort();
    }

    @Override
    public StreamExpression toExpression(StreamFactory factory) throws IOException {
        StreamExpression expression = new StreamExpression(factory.getFunctionName(this.getClass()));
        if (!(this.stream instanceof Expressible)) {
            throw new IOException("This SortStream contains a non-expressible TupleStream - it cannot be converted to an expression");
        }

        expression.addParameter(((Expressible)this.stream).toExpression(factory));
        expression.addParameter(new StreamExpressionNamedParameter("json", this.JSONOptions.toString()));

        return expression;
    }

    @Override
    public Explanation toExplanation(StreamFactory streamFactory) throws IOException {
        return this.stream.toExplanation(streamFactory);
    }
}
