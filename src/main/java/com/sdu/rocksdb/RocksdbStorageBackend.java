package com.sdu.rocksdb;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

/**
 * @author hanhan.zhang
 * */
public class RocksdbStorageBackend implements StorageBackend {

  static {
    // loads the RocksDB c++ library
    RocksDB.loadLibrary();
  }

  private RocksDB db;

  private RocksdbStorageBackend(String path) {

    /*
     * the Options class contains a set of configurable DB options
     * that determines the behaviour of the database.
     * */
    Options options = new Options()
        .setCreateIfMissing(true);

    try {
      db = RocksDB.open(options, path);
    } catch (RocksDBException e) {
      // ignore
      System.out.println("");
    }

  }

}
