package com.biologis.solr.client.solrj.io.stream;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.FieldComparator;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.eval.RecursiveBooleanEvaluator;
import org.apache.solr.client.solrj.io.eval.StreamEvaluator;
import org.apache.solr.client.solrj.io.stream.StreamContext;
import org.apache.solr.client.solrj.io.stream.TupleStream;
import org.apache.solr.client.solrj.io.stream.expr.*;


 public class SortHavingStream extends TupleStream implements Expressible {

     private TupleStream stream;
     private RecursiveBooleanEvaluator evaluator;
     private StreamComparator comparator;
     private StreamContext streamContext;
     private Worker worker;

     public SortHavingStream(TupleStream stream, RecursiveBooleanEvaluator evaluator, StreamComparator comp)
             throws IOException {
         init(stream, evaluator, comp);
     }

     public SortHavingStream(StreamExpression expression, StreamFactory factory) throws IOException {

         List<StreamExpression> streamExpressions = factory.getExpressionOperandsRepresentingTypes(expression, Expressible.class, TupleStream.class);
         List<StreamExpression> evaluatorExpressions = factory.getExpressionOperandsRepresentingTypes(expression, RecursiveBooleanEvaluator.class);
         StreamExpressionNamedParameter byExpression = factory.getNamedOperand(expression, "by");

         if (expression.getParameters().size() != streamExpressions.size() + 2) {
             throw new IOException(
                     String.format(Locale.ROOT, "Invalid expression %s - unknown operands found", expression));
         }

         if (streamExpressions.size() != 1) {
             throw new IOException(
                     String.format(Locale.ROOT, "Invalid expression %s - expecting a single stream but found %d",
                             expression, streamExpressions.size()));
         }

         if (byExpression == null || !(byExpression.getParameter() instanceof StreamExpressionValue)) {
             throw new IOException(String.format(Locale.ROOT, "Invalid expression %s - expecting single 'by' parameter listing fields to sort over but didn't find one", expression));
         }

         StreamEvaluator evaluator = null;
         if (evaluatorExpressions != null && evaluatorExpressions.size() == 1) {
             StreamExpression ex = evaluatorExpressions.get(0);
             evaluator = factory.constructEvaluator(ex);
             if (!(evaluator instanceof RecursiveBooleanEvaluator)) {
                 throw new IOException("The SortHavingStream requires a RecursiveBooleanEvaluator. A StreamEvaluator was provided.");
             }
         } else {
             throw new IOException("The SortHavingStream requires a RecursiveBooleanEvaluator.");
         }

         init(
                 factory.constructStream(streamExpressions.get(0)),
                 (RecursiveBooleanEvaluator) evaluator,
                 factory.constructComparator(((StreamExpressionValue) byExpression.getParameter()).getValue(), FieldComparator.class));
     }

     private void init(TupleStream stream, RecursiveBooleanEvaluator evaluator, StreamComparator comp)
             throws IOException {
         this.stream = stream;
         this.evaluator = evaluator;
         this.comparator = comp;

         // standard java modifier merge sort
         worker = new Worker() {

             private LinkedList<Tuple> tuples = new LinkedList<Tuple>();
             private Tuple eofTuple;

             public void readStream(TupleStream stream) throws IOException {
                 Tuple tuple = stream.read();
                     while (!tuple.EOF) {
                         tuples.add(tuple);
                         tuple = stream.read();
                     }
                 eofTuple = tuple;
             }

             public void sort() {
                 tuples.sort(comparator);
             }

             public Tuple read() {
                 if (tuples.isEmpty()) {
                     return eofTuple;
                 }
                 return tuples.removeFirst();
             }
         };
     }

     @Override
     public StreamExpression toExpression(StreamFactory factory) throws IOException {
         return toExpression(factory, true);
     }

     private StreamExpression toExpression(StreamFactory factory, boolean includeStreams) throws IOException {

         // function name
         StreamExpression expression = new StreamExpression(factory.getFunctionName(this.getClass()));

         // stream
         if(includeStreams){
             expression.addParameter(((Expressible) stream).toExpression(factory));
         }
         else{
             expression.addParameter("<stream>");
         }

         // evaluator
         if (evaluator instanceof Expressible) {
             expression.addParameter(evaluator.toExpression(factory));
         } else {
             throw new IOException("This SortHavingStream contains a non-expressible evaluator - it cannot be converted to an expression");
         }

         // comparator (sort by)
         if (comparator instanceof Expressible) {
             expression.addParameter(
                     new StreamExpressionNamedParameter("by", ((Expressible) comparator).toExpression(factory)));
         } else {
             throw new IOException("This SortStream contains a non-expressible equalitor - it cannot be converted to an expression");
         }

         return expression;
     }

     @Override
     public Explanation toExplanation(StreamFactory factory) throws IOException {

         return new StreamExplanation(getStreamNodeId().toString())
                 .withChildren(new Explanation[] { stream.toExplanation(factory) })
                 .withFunctionName(factory.getFunctionName(this.getClass()))
                 .withImplementingClass(this.getClass().getName())
                 .withExpressionType(Explanation.ExpressionType.STREAM_DECORATOR)
                 .withExpression(toExpression(factory, false).toString())
                 .withHelpers(new Explanation[] { evaluator.toExplanation(factory) })
                 .withHelper(comparator.toExplanation(factory));
     }

     public void setStreamContext(StreamContext context) {
         this.streamContext = context;
         this.stream.setStreamContext(context);
         this.evaluator.setStreamContext(context);
     }

     public List<TupleStream> children() {
         List<TupleStream> l = new ArrayList<TupleStream>();
         l.add(stream);
         return l;
     }

     public void open() throws IOException {
         stream.open();

         worker.readStream(stream);
         worker.sort();
     }

     public void close() throws IOException {
         stream.close();
     }

     public Tuple read() throws IOException {
         while (true) {
             Tuple tuple = worker.read();
             if (tuple.EOF) {
                 return tuple;
             }

             streamContext.getTupleContext().clear();
             if ((boolean)evaluator.evaluate(tuple)) {
                 return tuple;
             }
         }
     }

     public StreamComparator getStreamSort() {
         return comparator;
     }

     public int getCost() {
         return 0;
     }

     private interface Worker {
         public void readStream(TupleStream stream) throws IOException;

         public void sort();

         public Tuple read();
     }
 }