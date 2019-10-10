package com.sdu.rocksdb;

import com.sdu.rocksdb.utils.RocksDBOperationUtils;
import com.sdu.rocksdb.utils.RocksIteratorWrapper;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author hanhan.zhang
 * */
public class RocksDBStorageBackend implements StorageBackend {

  private static final Logger LOG = LoggerFactory.getLogger(RocksDBStorageBackend.class);

  static {
    // loads the RocksDB c++ library
    RocksDB.loadLibrary();
  }

  private final WriteOptions writeOptions;
  private final Function<String, ColumnFamilyOptions> columnFamilyOptionsFactory;

  private RocksDB db;

  private final Map<String, ColumnFamilyHandle> namespaceInformation;

  private RocksDBStorageBackend(String path, Function<String, ColumnFamilyOptions> factory) {
    this.writeOptions = new WriteOptions().setDisableWAL(false);
    this.columnFamilyOptionsFactory = factory;

    this.namespaceInformation = new HashMap<>();

    /*
     * the Options class contains a set of configurable DB options
     * that determines the behaviour of the database.
     * */
    Options options = new Options().setCreateIfMissing(true);
    options.setLogger(new RocksDBLogger(options, LOG));

    try {
      db = RocksDB.open(options, path);
    } catch (RocksDBException e) {
      // ignore
      LOG.error("initialize RocksDB failure !!!", e);
    }

  }

  @Override
  public void put(String namespace, byte[] key, byte[] value) throws IOException {
    ColumnFamilyHandle columnFamilyHandle = namespaceInformation.computeIfAbsent(namespace, k ->
        RocksDBOperationUtils.createColumnFamily(db, k, columnFamilyOptionsFactory));

    try {
      db.put(columnFamilyHandle, writeOptions, key, value);
    } catch (RocksDBException e) {
      throw new IOException("put data to RocksDB failure !!!", e);
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

    Map<byte[], byte[]> keyValues = new HashMap<>();
    RocksIteratorWrapper iterator = new RocksIteratorWrapper(db.newIterator(columnFamilyHandle));
    try {
      while (iterator.isValid()) {
        keyValues.put(iterator.key(), iterator.value());
        iterator.next();
      }
      return keyValues;
    } catch (Exception e) {
      throw new IOException("iterator RocksDB data failure", e);
    }

  }

  @Override
  public void snapshot(String namespace) throws IOException {

  }

}
