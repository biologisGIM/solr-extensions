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


public class AddFieldTest {

    StreamFactory factory;
    Map<String, Object> values;

    public AddFieldTest() {
        super();

        factory = new StreamFactory()
                .withFunctionName("addField", AddField.class);
        values = new HashedMap();
    }

    @Test
    public void addFieldNewFieldExpression() throws IOException {

        new AddField(StreamExpressionParser.parse("addField(value=\"fValue\", as=\"fieldA\")"), factory);
    }

    @Test(expected = NullPointerException.class)
    public void addFieldWithNullFiledValueExpression() throws IOException {

        new AddField(StreamExpressionParser.parse("addField(value=, as=\"fieldA\")"), factory);
    }

    @Test(expected = NullPointerException.class)
    public void addFieldWithNullFieldNameExpression() throws IOException {

        new AddField(StreamExpressionParser.parse("addField(value=\"fValue\", as)"), factory);
    }

    @Test(expected = IOException.class)
    public void addFieldWithoutParameters() throws IOException {
        new AddField(StreamExpressionParser.parse("addField()"), factory);
    }

    @Test
    public void addFieldNewField() throws IOException {
        StreamOperation operation;
        Tuple tuple;
        boolean notExistYet = true;

        operation = new AddField(StreamExpressionParser.parse("addField(value=\"fValue\", as=\"fieldA\")"),factory);
        values.clear();
        values.put("fieldA", notExistYet);
        tuple = new Tuple(values);
        operation.operate(tuple);

        Assert.assertEquals("fValue", tuple.get("fieldA"));
    }

    @Test
    public void addFieldNewField2() throws IOException {
        StreamOperation operation;
        Tuple tuple;

        String as = new String();


        operation = new AddField(as, "fValue");
        values.clear();
        values.put(as, "as");
        tuple = new Tuple(values);
        operation.operate(tuple);

        Assert.assertEquals("fValue", tuple.get(as));
    }
}