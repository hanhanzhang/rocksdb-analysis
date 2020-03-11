package com.sdu.rocksdb.utils;

import org.rocksdb.Cache;
import org.rocksdb.LRUCache;
import org.rocksdb.WriteBufferManager;

public class RocksDBMemoryControllerUtils {

  private RocksDBMemoryControllerUtils() {

  }

  private static long calculateActualCacheCapacity(long totalMemorySize, double writeBufferRatio) {
    return (long) ((3 - writeBufferRatio) * totalMemorySize / 3);
  }


  public static Cache createCache(long totalMemorySize, double writeBufferRatio, double highPriorityPoolRatio) {
    long cacheCapacity = calculateActualCacheCapacity(totalMemorySize, writeBufferRatio);
    return new LRUCache(cacheCapacity, -1, false, highPriorityPoolRatio);
  }


  private static long calculateWriteBufferManagerCapacity(long totalMemorySize, double writeBufferRatio) {
    return (long) (2 * totalMemorySize * writeBufferRatio / 3);
  }

  public static WriteBufferManager createWriteBufferManager(long totalMemorySize, double writeBufferRatio, Cache cache) {
    long writeBufferManagerCapacity = calculateWriteBufferManagerCapacity(totalMemorySize, writeBufferRatio);
    return new WriteBufferManager(writeBufferManagerCapacity, cache);
  }

}
