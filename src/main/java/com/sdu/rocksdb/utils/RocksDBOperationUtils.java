package com.sdu.rocksdb.utils;

import java.nio.charset.StandardCharsets;
import java.util.function.Function;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

/**
 * @author hanhan.zhang
 * */
public class RocksDBOperationUtils {

  /** The name of the merge operator in RocksDB. Do not change except you know exactly what you do. */
  private static final String MERGE_OPERATOR_NAME = "stringappendtest";

  public static ColumnFamilyHandle createColumnFamily(RocksDB db, String stateName, Function<String, ColumnFamilyOptions> columnFamilyOptionsFactory) {
    /*
     * RocksDB数据存储基于ColumnFamily, 默认数据存储在名为'default'的ColumnFamily列族中
     * */

    // step1: 构建列族描述, ColumnFamilyDescriptor
    ColumnFamilyOptions options = columnFamilyOptionsFactory.apply(stateName).setMergeOperatorName(MERGE_OPERATOR_NAME);
    byte[] nameBytes = stateName.getBytes(StandardCharsets.UTF_8);
    ColumnFamilyDescriptor descriptor = new ColumnFamilyDescriptor(nameBytes, options);

    // step2: 构建列族, ColumnFamilyHandle
    try {
      return db.createColumnFamily(descriptor);
    } catch (RocksDBException e) {
      IOUtils.closeQuietly(descriptor.getOptions());
      throw new RuntimeException("create RocksDb ColumnFamilyHandle failure", e);
    }
  }

}
