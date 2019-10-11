package com.sdu.rocksdb.serializer;

import java.io.IOException;

public interface RocksDBDataSerializer {

  void serializer(byte[] bytes) throws IOException;

}
