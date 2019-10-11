package com.sdu.rocksdb.serializer;

import java.io.IOException;

public interface DataSerializer {

  void serializer(byte[] bytes) throws IOException;

}
