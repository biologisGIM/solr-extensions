package com.biologis.solr.client.solrj.io.stream;

import com.biologis.solr.client.solrj.io.util.*;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.stream.StreamContext;
import org.apache.solr.client.solrj.io.stream.TupleStream;
import org.apache.solr.client.solrj.io.stream.expr.Explanation;
import org.apache.solr.client.solrj.io.stream.expr.Expressible;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

import java.io.IOException;
import java.util.List;

/**
 * Parent TupleStream to all our custom Streaming expressions. It provides functionality for caching the SEs
 */

abstract class CachedTupleStream extends TupleStream {

    private TupleContainer cachedStream;
    private CacheManager cacheManager;
    protected TupleStream stream;
    protected StreamFactory factory;
    public StreamContext streamContext;

    @Override
    public void open() throws IOException {
        if (this.cacheManager == null){
            this.cacheManager = new CacheManager(new CacheFactory());
        }
        this.cachedStream = this.cacheManager.getCashedStream((Expressible)this, this.factory, this.streamContext);

        if (this.cachedStream == null){
            this.transformAndLoad();
        }
    }

    private void transformAndLoad() throws IOException {
        transform();

        TupleContainer result = new TupleContainer(this);

        this.cachedStream = this.cacheManager.putInCache((Expressible)this, this.factory, result);
    }

    protected abstract void transform() throws IOException;

    @Override
    public void close() throws IOException {
        close();
    }

    @Override
    public Tuple read() throws IOException {
        if (this.cachedStream != null){
            return this.cachedStream.read();
        }
        return readNormal();
    }

    protected abstract Tuple readNormal() throws IOException;

    @Override
    public StreamComparator getStreamSort() {
        return getStreamSort();
    }

    @Override
    public Explanation toExplanation(StreamFactory factory) throws IOException {
        return toExplanation(factory);
    }

    @Override
    public void setStreamContext(StreamContext context) {
        setStreamContext(context);
    }

    @Override
    public List<TupleStream> children() {
        return children();
    }
}
