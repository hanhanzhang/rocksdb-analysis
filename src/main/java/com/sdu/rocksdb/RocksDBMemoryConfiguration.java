package com.sdu.rocksdb;

import lombok.Data;

@Data
public class RocksDBMemoryConfiguration {

  // 每个Slot分配给RocksDB的总内存, 这部分内存来自MemoryManager
  private long totalMemorySize;

  // RocksDB写缓冲区比例
  private double writeBufferRatio;

  // The high priority pool ratio of cache
  private double highPriorityPoolRatio;

}
