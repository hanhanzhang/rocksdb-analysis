package com.sdu.rocksdb.snapshot;

import com.sdu.rocksdb.serializer.DataSerializer;
import com.sdu.rocksdb.utils.DataHandleID;
import com.sun.xml.internal.messaging.saaj.util.ByteOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import org.rocksdb.Checkpoint;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

public class RocksDBIncrementSnapshotStrategy implements SnapshotStrategy {

  private static final String SST_FILE_SUFFIX = ".sst";

  private final RocksDB db;
  private final String pathTemplate;

  /** 已完成的快照的文件句柄集合 */
  private final SortedMap<Long, Set<DataHandleID>> materializedSstFiles;

  private long lastCompletedCheckpointId = 0;

  public RocksDBIncrementSnapshotStrategy(RocksDB db, String path) {
    this.db = db;
    this.pathTemplate = path + "/chk-%d";

    this.materializedSstFiles = new TreeMap<>();
  }

  @Override
  public void snapshot(String namespace, DataSerializer serializer) throws IOException {
    try {
      /*
       * 创建硬链接
       *
       * 软链接和硬链接的区别参考: https://www.jianshu.com/p/dde6a01c4094
       * **/
      String localBackupDirectoryPath = String.format(pathTemplate, lastCompletedCheckpointId + 1);
      Checkpoint checkpoint = Checkpoint.create(db);
      checkpoint.createCheckpoint(localBackupDirectoryPath);

      File localBackupDirectory = new File(localBackupDirectoryPath);
      assert localBackupDirectory.exists() : "RocksDB checkpoint failure.";
      File[] files = localBackupDirectory.listFiles();
      assert files != null && files.length > 0;

      // step1: 上次快照的文件集合
      Set<DataHandleID> baseSstFiles = materializedSstFiles.get(lastCompletedCheckpointId);

      // step2: 选取本次快照的文件集合(剔除上次做的快照文件集合)
      Map<DataHandleID, File> sstFilePaths = new HashMap<>();
      Map<DataHandleID, File> miscFilePaths = new HashMap<>();
      for (File file : files) {
        final String fileName = file.getName();
        final DataHandleID stateHandleID = new DataHandleID(fileName);

        if (fileName.endsWith(SST_FILE_SUFFIX)) {
          final boolean existsAlready = baseSstFiles != null && baseSstFiles.contains(stateHandleID);

          if (!existsAlready) {
            sstFilePaths.put(stateHandleID, file);
          }
        } else {
          miscFilePaths.put(stateHandleID, file);
        }
      }

      // step3: 开始做快照
      uploadFilesToCheckpointFs(sstFilePaths, serializer);
      uploadFilesToCheckpointFs(miscFilePaths, serializer);

      // step4: 快照版本号自增, 记录本次快照文件
      lastCompletedCheckpointId += 1;
      if (baseSstFiles != null) {
        Set<DataHandleID> newDataHandleIDs = new HashSet<>();
        newDataHandleIDs.addAll(sstFilePaths.keySet());
        newDataHandleIDs.addAll(baseSstFiles);
        materializedSstFiles.put(lastCompletedCheckpointId, newDataHandleIDs);
      } else {
        materializedSstFiles.put(lastCompletedCheckpointId, sstFilePaths.keySet());
      }


    } catch (RocksDBException e) {
      throw new IOException("snapshot failure when create checkpoint", e);
    }

  }

  private static void uploadFilesToCheckpointFs(Map<DataHandleID, File> files, DataSerializer serializer) throws IOException {
    for (Entry<DataHandleID, File> entry : files.entrySet()) {
      FileInputStream inputStream = new FileInputStream(entry.getValue());
      ByteOutputStream outputStream = new ByteOutputStream();

      // 16KB
      byte[] buffer = new byte[16 * 1024];

      while (true) {
        int numBytes = inputStream.read(buffer);
        if (numBytes == -1) {
          break;
        }
        outputStream.write(buffer, 0, numBytes);
      }

      outputStream.flush();
      serializer.serializer(outputStream.getBytes());
    }
  }

}
