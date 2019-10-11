package com.sdu.rocksdb.snapshot;

import com.sdu.rocksdb.serializer.DataSerializer;
import com.sdu.rocksdb.utils.RocksIteratorWrapper;
import java.io.IOException;
import java.util.Map;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.Snapshot;

public class RocksDBFullSnapshotStrategy implements SnapshotStrategy{

  private final RocksDB db;
  private final Map<String, ColumnFamilyHandle> namespaceInformation;

  public RocksDBFullSnapshotStrategy(RocksDB db, Map<String, ColumnFamilyHandle> namespaceInformation) {
    this.db = db;
    this.namespaceInformation = namespaceInformation;
  }

  @Override
  public void snapshot(String namespace, DataSerializer serializer) throws IOException {
    ColumnFamilyHandle columnFamilyHandle = namespaceInformation.get(namespace);
    if (columnFamilyHandle == null) {
      return;
    }

    Snapshot snapshot = db.getSnapshot();

    // 读取快照数据
    ReadOptions readOptions = new ReadOptions();
    readOptions.setSnapshot(snapshot);
    RocksIteratorWrapper iterator = new RocksIteratorWrapper(db.newIterator(columnFamilyHandle, readOptions));
    try {
      for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
        serializer.serializer(iterator.key());
        serializer.serializer(iterator.value());
      }
    } catch (Exception e) {
      throw new IOException("RocksDB snapshot failure when iterator data", e);
    }
  }

}
