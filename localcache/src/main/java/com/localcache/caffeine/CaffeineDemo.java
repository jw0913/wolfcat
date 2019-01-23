package com.localcache.caffeine;

import com.alibaba.fastjson.JSON;
import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

/**
 * Caffeine配置说明：
 * initialCapacity=[integer]: 初始的缓存空间大小
 * maximumSize=[long]: 缓存的最大条数
 * maximumWeight=[long]: 缓存的最大权重
 * expireAfterAccess=[duration]: 最后一次写入或访问后经过固定时间过期
 * expireAfterWrite=[duration]: 最后一次写入后经过固定时间过期
 * refreshAfterWrite=[duration]: 创建缓存或者最近一次更新缓存后经过固定的时间间隔，刷新缓存
 * recordStats：开发统计功能
 * 注意：
 * expireAfterWrite和expireAfterAccess同时存在时，以expireAfterWrite为准。
 * maximumSize和maximumWeight不可以同时使用
 */
public class CaffeineDemo {
    public void baseCache(){
        Cache<String,Object> cache= Caffeine.newBuilder()
                .expireAfterWrite(10,TimeUnit.SECONDS)//最后一次写入后经过固定时间过期
                .maximumSize(1000)
                .build();
        String key="test";
        //根据key查询一个缓存，如果没有就返回null
        Object o=cache.getIfPresent(key);
        System.out.println(o);
        /**
         * 根据Key查询一个缓存，如果没有调用createExpensiveGraph方法，并将返回值保存到缓存。
         * 如果该方法返回Null则manualCache.get返回null，如果该方法抛出异常则manualCache.get抛出异常
         */
        o=cache.get(key,k->createCache(k));
        System.out.println(o);

        // 将一个值放入缓存，如果以前有值就覆盖以前的值
        cache.put("test2", "test2");
        // 删除一个缓存
        //cache.invalidate(key);
        ConcurrentMap<String, Object> map = cache.asMap();
        //cache.invalidate("test2");
        System.out.println(JSON.toJSONString(map));
    }

    /**
     * 同步加载（Loading）LoadingCache是使用CacheLoader来构建的缓存的值
     */
    public void loadingCache(){
        LoadingCache<String, Object> loadingCache = Caffeine.newBuilder()
                .maximumSize(10000)
                .expireAfterWrite(10, TimeUnit.MINUTES)
                .build(key -> createCache(key));
        String key = "name1";
        // 采用同步方式去获取一个缓存和上面的手动方式是一个原理。
        // 在build Cache的时候会提供一个createCache函数。
        // 查询并在缺失的情况下使用同步的方式来构建一个缓存
        Object graph = loadingCache.get(key);
        System.out.println(graph);
        // 获取组key的值返回一个Map
        List<String> keys = new ArrayList<>();
        keys.add(key);
        Map<String, Object> graphs = loadingCache.getAll(keys);
        System.out.println(graphs);
    }

    /**
     * 异步加载（Asynchronously Loading）
     * 默认使用ForkJoinPool.commonPool()来执行异步线程，但是可以通过Caffeine.executor(Executor) 方法来替换线程池
     */
    public void asyncLoadingCache(){
        AsyncLoadingCache<String, Object> asyncLoadingCache = Caffeine.newBuilder()
                .maximumSize(10000)
                .expireAfterWrite(10, TimeUnit.MINUTES)
                // Either: Build with a synchronous computation that is wrapped as asynchronous
                .buildAsync(key -> createCache(key));
        // Or: Build with a asynchronous computation that returns a future
        // .buildAsync((key, executor) -> createExpensiveGraphAsync(key, executor));

        String key = "name1";

        // 查询并在缺失的情况下使用异步的方式来构建缓存
        CompletableFuture<Object> graph = asyncLoadingCache.get(key);
        // 查询一组缓存并在缺失的情况下使用异步的方式来构建缓存
        List<String> keys = new ArrayList<>();
        keys.add(key);
        CompletableFuture<Map<String, Object>> graphs = asyncLoadingCache.getAll(keys);
        // 异步转同步
        LoadingCache loadingCache = asyncLoadingCache.synchronous();
    }
    public Object createCache(String k){
            return k;
    }

    public void test(){
        String x="全球购xxxxx全球购aaaaa全球购";
        String s=StringUtils.replace(x,"全球购","海囤全球");
        System.out.println(s);
    }

    public static void main(String[] args) {
        CaffeineDemo caffeineDemo=new CaffeineDemo();
        //caffeineDemo.baseCache();
        //caffeineDemo.loadingCache();
        //caffeineDemo.asyncLoadingCache();
        caffeineDemo.test();
    }
}


