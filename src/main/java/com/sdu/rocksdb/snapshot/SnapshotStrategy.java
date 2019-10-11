package com.sdu.rocksdb.snapshot;

import com.sdu.rocksdb.serializer.ByteArraySerializer;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author hanhan.zhang
 * */
public interface SnapshotStrategy {

  void snapshot(ByteArraySerializer serializer, DataOutput output) throws IOException;

}
