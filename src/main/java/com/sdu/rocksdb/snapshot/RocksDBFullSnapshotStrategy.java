package com.sdu.rocksdb.snapshot;

import com.sdu.rocksdb.serializer.DataSerializer;
import com.sdu.rocksdb.utils.RocksIteratorWrapper;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.Snapshot;

public class RocksDBFullSnapshotStrategy implements SnapshotStrategy {

  private final RocksDB db;
  private final Map<String, ColumnFamilyHandle> namespaceInformation;

  public RocksDBFullSnapshotStrategy(RocksDB db, Map<String, ColumnFamilyHandle> namespaceInformation) {
    this.db = db;
    this.namespaceInformation = namespaceInformation;
  }

  @Override
  public void snapshot(DataSerializer serializer) throws IOException {
    Snapshot snapshot = db.getSnapshot();

    // 读取快照数据
    ReadOptions readOptions = new ReadOptions();
    readOptions.setSnapshot(snapshot);

    for (Entry<String, ColumnFamilyHandle> entry : namespaceInformation.entrySet()) {
      RocksIteratorWrapper iterator = new RocksIteratorWrapper(db.newIterator(entry.getValue(), readOptions));
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

}
