package com.sdu.rocksdb.serializer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public interface TypeSerializer<T> {

  byte[] serializer(T obj) throws IOException;

  void serializer(T obj, DataOutput output) throws IOException;

  T deserializer(byte[] bytes) throws IOException;

  T deserializer(DataInput input) throws IOException;

}
