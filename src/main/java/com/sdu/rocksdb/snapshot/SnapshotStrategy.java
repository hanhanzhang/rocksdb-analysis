package com.sdu.rocksdb.snapshot;

import com.sdu.rocksdb.serializer.DataSerializer;
import java.io.IOException;

/**
 * @author hanhan.zhang
 * */
public interface SnapshotStrategy {

  void snapshot(String namespace, DataSerializer serializer) throws IOException;

}