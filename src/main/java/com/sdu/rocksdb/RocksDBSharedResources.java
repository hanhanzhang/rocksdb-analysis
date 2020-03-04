package com.sdu.rocksdb;

import org.rocksdb.Cache;
import org.rocksdb.WriteBufferManager;

public class RocksDBSharedResources implements AutoCloseable {

  private final Cache cache;

  private final WriteBufferManager writeBufferManager;

  public RocksDBSharedResources(Cache cache, WriteBufferManager writeBufferManager) {
    this.cache = cache;
    this.writeBufferManager = writeBufferManager;
  }

  public Cache getCache() {
    return cache;
  }

  public WriteBufferManager getWriteBufferManager() {
    return writeBufferManager;
  }

  @Override
  public void close() throws Exception {
    writeBufferManager.close();
    cache.close();
  }
}
