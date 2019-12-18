package com.biologis.solr.client.solrj.io.util;

import org.apache.solr.client.solrj.io.stream.StreamContext;

public interface ICacheFactory {

    public BiologisCache getCache(StreamContext context);


}
