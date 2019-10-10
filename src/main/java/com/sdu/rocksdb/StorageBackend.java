package com.sdu.rocksdb;

import java.io.IOException;
import java.util.Map;

/**
 * @author hanhan.zhang
 * */
public interface StorageBackend {

  void put(String namespace, byte[] key, byte[] value) throws IOException;

  byte[] get(String namespace, byte[] key) throws IOException;

  Map<byte[], byte[]> getAll(String namespace) throws IOException;

  void snapshot(String namespace) throws IOException;

}
