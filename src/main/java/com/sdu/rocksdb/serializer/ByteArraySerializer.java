package com.sdu.rocksdb.serializer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ByteArraySerializer implements TypeSerializer<byte[]> {

  public static final ByteArraySerializer INSTANCE = new ByteArraySerializer();

  private ByteArraySerializer() {

  }

  @Override
  public byte[] serializer(byte[] obj) throws IOException {
    return obj;
  }

  @Override
  public void serializer(byte[] bytes, DataOutput output) throws IOException {
    serializer(bytes, 0, bytes.length, output);
  }

  public void serializer(byte[] bytes, int offset, int len, DataOutput output) throws IOException {
    if (offset < 0 || offset >= bytes.length || len < 0 || len > bytes.length
        || offset + len >= bytes.length) {
      throw new IllegalArgumentException();
    }

    output.writeInt(len);
    output.write(bytes, offset, len);
  }

  @Override
  public byte[] deserializer(byte[] bytes) throws IOException {
    return bytes;
  }

  @Override
  public byte[] deserializer(DataInput input) throws IOException {
    int len = input.readInt();
    byte[] bytes = new byte[len];
    input.readFully(bytes);
    return bytes;
  }


}
