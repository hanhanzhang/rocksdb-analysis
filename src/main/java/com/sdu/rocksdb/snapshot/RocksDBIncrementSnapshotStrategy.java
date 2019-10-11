package com.sdu.rocksdb.snapshot;

import com.sdu.rocksdb.serializer.DataSerializer;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import org.rocksdb.Checkpoint;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

public class RocksDBIncrementSnapshotStrategy implements SnapshotStrategy {

  private final RocksDB db;
  private final String path;
  private final Map<String, ColumnFamilyHandle> namespaceInformation;

  public RocksDBIncrementSnapshotStrategy(RocksDB db, String path, Map<String, ColumnFamilyHandle> namespaceInformation) {
    this.db = db;
    this.path = path + File.pathSeparator + "chk";
    this.namespaceInformation = namespaceInformation;
  }

  @Override
  public void snapshot(String namespace, DataSerializer serializer) throws IOException {
    try {
      /*
       * 创建硬链接
       *
       * 软链接和硬链接的区别参考: https://www.jianshu.com/p/dde6a01c4094
       * **/
      Checkpoint checkpoint = Checkpoint.create(db);
      checkpoint.createCheckpoint(path);
    } catch (RocksDBException e) {
      throw new IOException("snapshot failure when create checkpoint", e);
    }

  }


}
