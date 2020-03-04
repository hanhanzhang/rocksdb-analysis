package com.sdu.rocksdb;

import com.sdu.rocksdb.utils.IOUtils;
import java.util.ArrayList;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.Cache;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.TableFormatConfig;

public class RocksDBResourceContainer implements AutoCloseable {

  private PredefinedOptions predefinedOptions;

  private RocksDBSharedResources sharedResources;

  private final ArrayList<AutoCloseable> handlesToClose;

  public RocksDBResourceContainer(PredefinedOptions predefinedOptions, RocksDBSharedResources sharedResources) {
    this.predefinedOptions = predefinedOptions;
    this.sharedResources = sharedResources;
    this.handlesToClose = new ArrayList<>();

  }

  public DBOptions getDbOptions() {
    DBOptions opt = predefinedOptions.createDBOptions();

    opt = opt.setCreateIfMissing(true);

    // 设置内存(buffer)
    if (sharedResources != null) {
      opt.setWriteBufferManager(sharedResources.getWriteBufferManager());
    }

    return opt;
  }

  public ColumnFamilyOptions getColumnOptions() {
    ColumnFamilyOptions opt = predefinedOptions.createColumnOptions(handlesToClose);
    handlesToClose.add(opt);

    if (sharedResources != null) {
      final Cache blockCache = sharedResources.getCache();
      TableFormatConfig tableFormatConfig = opt.tableFormatConfig();
      BlockBasedTableConfig blockBasedTableConfig;
      if (tableFormatConfig == null) {
        blockBasedTableConfig = new BlockBasedTableConfig();
      } else {
        blockBasedTableConfig = (BlockBasedTableConfig) tableFormatConfig;
      }
      blockBasedTableConfig.setBlockCache(blockCache);
      blockBasedTableConfig.setCacheIndexAndFilterBlocks(true);
      blockBasedTableConfig.setCacheIndexAndFilterBlocksWithHighPriority(true);
      blockBasedTableConfig.setPinL0FilterAndIndexBlocksInCache(true);
      opt.setTableFormatConfig(blockBasedTableConfig);
    }

    return opt;
  }

  @Override
  public void close() throws Exception {
    handlesToClose.forEach(IOUtils::closeQuietly);
    handlesToClose.clear();

    if (sharedResources != null) {
      sharedResources.close();
    }

  }

}
