package com.sdu.rocksdb.serializer;

import java.io.DataOutput;
import java.io.IOException;

public interface ByteArraySerializer extends ObjectSerializer<byte[]> {

  void serializer(byte[] bytes, int offset, int len, DataOutput output) throws IOException;

  @Override
  default void serializer(byte[] bytes, DataOutput output) throws IOException {
    serializer(bytes, 0, bytes.length, output);
  }
}
