package com.sdu.rocksdb;

import com.sdu.rocksdb.serializer.TypeSerializer;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

/**
 * @author hanhan.zhang
 * */
public interface StorageBackend<T extends TypeSerializer<?>> {

  void put(String namespace, byte[] key, byte[] value) throws IOException;

  void flush(String namespace) throws IOException;

  byte[] get(String namespace, byte[] key) throws IOException;

  Map<byte[], byte[]> getAll(String namespace) throws IOException;

  Map<byte[], byte[]> getAll() throws IOException;

  void snapshot(T serializer, DataOutput output) throws IOException;

}
