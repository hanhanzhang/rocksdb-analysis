package com.sdu.rocksdb;

import com.sdu.rocksdb.serializer.ByteArraySerializer;
import com.sdu.rocksdb.snapshot.RocksDBFullSnapshotStrategy;
import com.sdu.rocksdb.snapshot.RocksDBIncrementSnapshotStrategy;
import com.sdu.rocksdb.snapshot.SnapshotStrategy;
import com.sdu.rocksdb.snapshot.SnapshotType;
import com.sdu.rocksdb.utils.IOUtils;
import com.sdu.rocksdb.utils.RocksDBOperationUtils;
import com.sdu.rocksdb.utils.RocksIteratorWrapper;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.FlushOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author hanhan.zhang
 * */
public class RocksDBStorageBackend implements StorageBackend<ByteArraySerializer> {

  private static final Logger LOG = LoggerFactory.getLogger(RocksDBStorageBackend.class);

  static {
    // loads the RocksDB c++ library
    RocksDB.loadLibrary();
  }

  private final WriteOptions writeOptions;

  private RocksDB db;
  private SnapshotStrategy snapshotStrategy;
  private final RocksDBResourceContainer resourceContainer;

  private final List<ColumnFamilyHandle> columnFamilyHandles;

  private final Map<String, ColumnFamilyHandle> namespaceInformation;

  public RocksDBStorageBackend(String path, SnapshotType type) {
    this.writeOptions = new WriteOptions().setDisableWAL(false);

    this.columnFamilyHandles = new ArrayList<>(1);
    this.namespaceInformation = new HashMap<>();

    // RocksDBResourceContainer管理RocksDB的内存使用
    this.resourceContainer = new RocksDBResourceContainer(
        PredefinedOptions.DEFAULT, RocksDBOperationUtils.allocateSharedCachesIfConfigured());


    try {
      db = RocksDBOperationUtils.openDB(path, Collections.emptyList(), columnFamilyHandles,
          RocksDBOperationUtils.createColumnFamilyOptions(stateName -> resourceContainer.getColumnOptions(), "default"),
          resourceContainer.getDbOptions());

      switch (type) {
        case FULL:
          snapshotStrategy = new RocksDBFullSnapshotStrategy(db, namespaceInformation);
          break;
        case INCREMENT:
          snapshotStrategy = new RocksDBIncrementSnapshotStrategy(db, path);
          break;
      }
    } catch (IOException e) {
      // ignore
      LOG.error("initialize RocksDB failure !!!", e);
    }

  }

  @Override
  public void put(String namespace, byte[] key, byte[] value) throws IOException {
    ColumnFamilyHandle columnFamilyHandle = namespaceInformation.computeIfAbsent(namespace, k ->
        RocksDBOperationUtils.createColumnFamily(db, k, name -> resourceContainer.getColumnOptions()));
    columnFamilyHandles.add(columnFamilyHandle);

    try {
      db.put(columnFamilyHandle, writeOptions, key, value);
    } catch (RocksDBException e) {
      throw new IOException("put data to RocksDB failure !!!", e);
    }

  }

  @Override
  public void flush(String namespace) throws IOException {
    ColumnFamilyHandle columnFamilyHandle = namespaceInformation.get(namespace);
    if (columnFamilyHandle == null) {
      return;
    }

    FlushOptions flushOptions = new FlushOptions();
    flushOptions.setAllowWriteStall(true);
    flushOptions.setWaitForFlush(true);

    try {
      db.flush(flushOptions, columnFamilyHandle);
    } catch (RocksDBException e) {
      throw new IOException("flush data to RocksDB failure !!!", e);
    }
  }

  @Override
  public byte[] get(String namespace, byte[] key) throws IOException {
    ColumnFamilyHandle columnFamilyHandle = namespaceInformation.get(namespace);
    if (columnFamilyHandle == null) {
      return null;
    }

    try {
      return db.get(columnFamilyHandle, key);
    } catch (RocksDBException e) {
      throw new IOException("get data from RocksDB failure !!!", e);
    }
  }

  @Override
  public Map<byte[], byte[]> getAll(String namespace) throws IOException {
    ColumnFamilyHandle columnFamilyHandle = namespaceInformation.get(namespace);
    if (columnFamilyHandle == null) {
      return Collections.emptyMap();
    }

    RocksIteratorWrapper iterator = new RocksIteratorWrapper(db.newIterator(columnFamilyHandle));
    return getData(iterator);
  }

  @Override
  public Map<byte[], byte[]> getAll() throws IOException {
    // TODO: 如何使用?
    RocksIteratorWrapper iterator = new RocksIteratorWrapper(db.newIterator());
    return getData(iterator);
  }

  @Override
  public void snapshot(ByteArraySerializer serializer, DataOutput output) throws IOException {
    snapshotStrategy.snapshot(serializer, output);
  }

  @Override
  public void close() throws IOException {
    for (ColumnFamilyHandle columnFamilyHandle : columnFamilyHandles) {
      IOUtils.closeQuietly(columnFamilyHandle);
    }
  }

  public RocksIteratorWrapper getRocksIterator(String namespace) {
    ColumnFamilyHandle columnFamilyHandle = namespaceInformation.get(namespace);
    if (columnFamilyHandle == null) {
      return null;
    }

    return new RocksIteratorWrapper(db.newIterator(columnFamilyHandle));
  }

  private static Map<byte[], byte[]> getData(RocksIteratorWrapper iterator) throws IOException {
    Map<byte[], byte[]> keyValues = new HashMap<>();
    try {
      for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
        keyValues.put(iterator.key(), iterator.value());
      }
      return keyValues;
    } catch (Exception e) {
      throw new IOException("iterator RocksDB data failure", e);
    } finally {
      iterator.close();
    }
  }
}
