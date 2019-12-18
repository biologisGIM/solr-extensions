package com.biologis.solr.client.solrj.io.util;

import org.apache.commons.codec.binary.Hex;
import org.apache.solr.client.solrj.io.stream.StreamContext;
import org.apache.solr.client.solrj.io.stream.expr.Expressible;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class CacheManager {

    private static BiologisCache cache;
    private ICacheFactory cacheFactory;

    public CacheManager(ICacheFactory cacheFactory){
        this.cacheFactory = cacheFactory;
    }

    private String getCacheKey(Expressible stream, StreamFactory factory) throws IOException {
        try{
            byte[] md5Message = stream.toExpression(factory).toString().getBytes("UTF-8");
            byte[] md5Hash = MessageDigest.getInstance("MD5").digest(md5Message);
            return stream.getClass().getSimpleName() + Hex.encodeHexString(md5Hash);
        }
        catch (NoSuchAlgorithmException e){
            //
        }
        return null;
    }

    public TupleContainer getCashedStream(Expressible stream, StreamFactory factory, StreamContext context) throws IOException{
        if (this.cache == null){
            this.cache = cacheFactory.getCache(context);
        }
        Object result = this.cache.get(this.getCacheKey(stream, factory));
        if (result == null){
            return null;
        }
        return ((TupleContainer)result).clone();
    }

    public TupleContainer putInCache(Expressible stream, StreamFactory factory, TupleContainer obj) throws IOException{
        if (this.cache == null){
            throw new IOException("Cache not instantiated");
        }
        cache.put(this.getCacheKey(stream, factory), obj);

        return obj.clone();
    }

}
