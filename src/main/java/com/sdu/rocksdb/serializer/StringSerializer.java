package com.sdu.rocksdb.serializer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class StringSerializer implements TypeSerializer<String> {

  public static final StringSerializer INSTANCE = new StringSerializer();

  private static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;

  private StringSerializer() {

  }

  @Override
  public byte[] serializer(String obj) throws IOException {
    return obj.getBytes(DEFAULT_CHARSET);
  }

  @Override
  public void serializer(String obj, DataOutput output) throws IOException {
    byte[] bytes = obj.getBytes(DEFAULT_CHARSET);
    output.write(bytes.length);
    output.write(bytes);

  }

  @Override
  public String deserializer(byte[] bytes) throws IOException {
    return new String(bytes, DEFAULT_CHARSET);
  }

  @Override
  public String deserializer(DataInput input) throws IOException {
    int length = input.readInt();
    byte[] bytes = new byte[length];
    input.readFully(bytes);
    return new String(bytes, DEFAULT_CHARSET);
  }

}
