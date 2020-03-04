package com.sdu.rocksdb.utils;

import com.sdu.rocksdb.RocksDBSharedResources;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

/**
 * @author hanhan.zhang
 * */
public class RocksDBOperationUtils {

  /** The name of the merge operator in RocksDB. Do not change except you know exactly what you do. */
  private static final String MERGE_OPERATOR_NAME = "stringappendtest";

  public static RocksDBSharedResources allocateSharedCachesIfConfigured() {
    // TODO:
    return null;
  }


  public static RocksDB openDB(String path,
      List<ColumnFamilyDescriptor> columnFamilyDescriptors,
      List<ColumnFamilyHandle> columnFamilyHandles,
      ColumnFamilyOptions columnFamilyOptions,
      DBOptions dbOptions) throws IOException  {

    List<ColumnFamilyDescriptor> newColumnFamilyDescriptors =
        new ArrayList<>(1 + columnFamilyDescriptors.size());

    // we add the required descriptor for the default CF in FIRST position, see
    // https://github.com/facebook/rocksdb/wiki/RocksJava-Basics#opening-a-database-with-column-families

    newColumnFamilyDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, columnFamilyOptions));
    newColumnFamilyDescriptors.addAll(columnFamilyDescriptors);

    RocksDB dbRef;

    try {
      dbRef = RocksDB.open(dbOptions, path, newColumnFamilyDescriptors, columnFamilyHandles);
    } catch (RocksDBException e) {
      IOUtils.closeQuietly(columnFamilyOptions);
      columnFamilyDescriptors.forEach((cfd) -> IOUtils.closeQuietly(cfd.getOptions()));
      throw new IOException("Error while opening RocksDB instance.", e);
    }

    return dbRef;
  }

  public static ColumnFamilyOptions createColumnFamilyOptions(
      Function<String, ColumnFamilyOptions> columnFamilyOptionsFactory, String stateName) {

    // ensure that we use the right merge operator, because other code relies on this
    return columnFamilyOptionsFactory.apply(stateName).setMergeOperatorName(MERGE_OPERATOR_NAME);
  }

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
