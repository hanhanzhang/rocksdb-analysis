package com.sdu.rocksdb;

import java.util.Collection;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompactionStyle;
import org.rocksdb.DBOptions;
import org.rocksdb.InfoLogLevel;

public enum PredefinedOptions {

  DEFAULT {
    @Override
    public DBOptions createDBOptions() {
      // TODO:
      return new DBOptions()
          .setUseFsync(false)
          .setInfoLogLevel(InfoLogLevel.HEADER_LEVEL)
          .setStatsDumpPeriodSec(0)
          .setIncreaseParallelism(4)
          .setMaxOpenFiles(-1);
    }

    @Override
    public ColumnFamilyOptions createColumnOptions(Collection<AutoCloseable> handlesToClose) {
      final long blockCacheSize = 256 * 1024 * 1024;
      final long blockSize = 128 * 1024;
      final long targetFileSize = 256 * 1024 * 1024;
      final long writeBufferSize = 64 * 1024 * 1024;

      BloomFilter bloomFilter = new BloomFilter();
      handlesToClose.add(bloomFilter);

      return new ColumnFamilyOptions()
          .setCompactionStyle(CompactionStyle.LEVEL)
          .setLevelCompactionDynamicLevelBytes(true)
          .setTargetFileSizeBase(targetFileSize)
          .setMaxBytesForLevelBase(4 * targetFileSize)
          .setWriteBufferSize(writeBufferSize)
          .setMinWriteBufferNumberToMerge(3)
          .setMaxWriteBufferNumber(4)
          .setTableFormatConfig(
              new BlockBasedTableConfig()
                  .setBlockCacheSize(blockCacheSize)
                  .setBlockSize(blockSize)
                  .setFilter(bloomFilter)
          );
    }
  };

  public abstract DBOptions createDBOptions();

  public abstract ColumnFamilyOptions createColumnOptions(Collection<AutoCloseable> handlesToClose);
}
