package com.biologis.solr.client.solrj.io.util;

import org.apache.solr.client.solrj.io.stream.StreamContext;
import org.apache.solr.core.SolrCore;
import org.apache.solr.search.CacheConfig;
import org.apache.solr.search.SolrCache;

/**
 * Provides for getting unique static instance of the cache implementation.
 */
public class CacheFactory implements ICacheFactory{

    public BiologisCache getCache(StreamContext context){
        return createCacheInstance(context);
    }

    private static BiologisCache createCacheInstance(StreamContext context){
        CacheConfig cc = getCacheClass(context);
        BiologisCache cache =  ((BiologisCache)cc.newInstance());

        cache.setState(SolrCache.State.LIVE);

        return cache;
    }

    private static CacheConfig getCacheClass(StreamContext context){
        return ((SolrCore)context.get("solr-core")).getSolrConfig().userCacheConfigs.get("biologisCache");
    }

}
