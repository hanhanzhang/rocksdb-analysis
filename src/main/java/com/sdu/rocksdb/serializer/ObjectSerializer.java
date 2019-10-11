package com.sdu.rocksdb.serializer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public interface ObjectSerializer<T> {

  void serializer(T obj, DataOutput output) throws IOException;

  T deserializer(DataInput input) throws IOException;

}
