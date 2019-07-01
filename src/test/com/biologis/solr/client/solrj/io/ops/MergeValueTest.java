package com.biologis.solr.client.solrj.io.ops;

import org.apache.commons.collections.map.HashedMap;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.ops.StreamOperation;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionParser;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

public class MergeValueTest {

    StreamFactory factory;
    Map<String, Object> values;


    public MergeValueTest() {
        super();

        factory = new StreamFactory()
                .withFunctionName("mergeValue", MergeValue.class);
        values = new HashedMap();

    }

    @Test
    public void mergeValueToAField() throws IOException {

        Tuple tuple;
        StreamOperation operation;

        operation = new MergeValue("fieldA", "mValue", ",");
        values.clear();
        values.put("fieldA", "fValue");
        tuple = new Tuple(values);
        operation.operate(tuple);

//        Assert.assertNotNull(tuple.get("fieldA"));
        Assert.assertEquals("fValue,mValue", tuple.get("fieldA"));
    }

    @Test
    public void mergeValueWithFieldThatHasNullValue() throws IOException {
        Tuple tuple;
        StreamOperation operation;

        operation = new MergeValue("fieldA", "mValue", ",");
        values.clear();
        values.put("fieldA", null);
        tuple = new Tuple(values);
        operation.operate(tuple);

        Assert.assertEquals("null,mValue", tuple.get("fieldA"));
    }

    /*///////////////////////////////////
    Tests (using StreamExpressionParser)
     //////////////////////////////////*/

    @Test
    public void mergeValueToAFieldExpression() throws IOException {
        Tuple tuple;
        StreamOperation operation;

        operation = new MergeValue(StreamExpressionParser.parse("mergeValue(field=\"fieldA\", mergeValue=\"mValue\", delim=\",\")"), factory);
        values.clear();
        values.put("fieldA", "fValue");
        tuple = new Tuple(values);
        operation.operate(tuple);

        Assert.assertNotNull(tuple.get("fieldA"));
        Assert.assertEquals("fValue,mValue", tuple.get("fieldA"));
    }

    @Test(expected = NullPointerException.class)
    public void mergeValueWithNullValueExpression() throws IOException {
        Tuple tuple;
        StreamOperation operation;

        operation = new MergeValue(StreamExpressionParser.parse("mergeValue(field=\"fieldA\", mergeValue=, delim=\",\")"), factory);
        values.clear();
        values.put("fieldA", "fValue");
        tuple = new Tuple(values);
        operation.operate(tuple);

    }

    @Test(expected = NullPointerException.class)
    public void mergeValueWithNullDelimExpression() throws IOException {
        Tuple tuple;
        StreamOperation operation;

        operation = new MergeValue(StreamExpressionParser.parse("mergeValue(field=\"fieldA\", mergeValue=, delim=)"), factory);
        values.clear();
        values.put("fieldA", "fValue");
        tuple = new Tuple(values);
        operation.operate(tuple);

    }

    @Test(expected = IOException.class)
    public void mergeValueWithoutParameters() throws IOException {
        new MergeValue(StreamExpressionParser.parse("mergeValue()"), factory);
    }
}