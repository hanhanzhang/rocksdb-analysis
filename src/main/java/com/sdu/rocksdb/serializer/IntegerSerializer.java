package com.sdu.rocksdb.serializer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class IntegerSerializer implements TypeSerializer<Integer> {

  public static final IntegerSerializer INSTANCE = new IntegerSerializer();

  private IntegerSerializer() {

  }

  @Override
  public byte[] serializer(Integer obj) throws IOException {
    return new byte[]{
        (byte) ((obj >> 24) & 0xFF),
        (byte) ((obj >> 16) & 0xFF),
        (byte) ((obj >> 8) & 0xFF),
        (byte) (obj & 0xFF)
    };
  }

  @Override
  public void serializer(Integer obj, DataOutput output) throws IOException {
    output.write(obj);
  }

  @Override
  public Integer deserializer(byte[] bytes) throws IOException {
    return bytes[3] & 0xFF |
        (bytes[2] & 0xFF) << 8 |
        (bytes[1] & 0xFF) << 16 |
        (bytes[0] & 0xFF) << 24;
  }

  @Override
  public Integer deserializer(DataInput input) throws IOException {
    return input.readInt();
  }

}
