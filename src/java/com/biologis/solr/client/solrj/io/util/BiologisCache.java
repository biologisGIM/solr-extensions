package com.biologis.solr.client.solrj.io.util;

import org.apache.solr.search.FastLRUCache;

public class BiologisCache<K, V> extends FastLRUCache<K, V> {

    @Override
    public V get(K key) {
        return super.get(key);
    }
}
