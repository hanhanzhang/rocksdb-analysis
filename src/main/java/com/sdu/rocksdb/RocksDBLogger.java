package com.sdu.rocksdb;

import org.rocksdb.InfoLogLevel;
import org.rocksdb.Logger;
import org.rocksdb.Options;

public class RocksDBLogger extends Logger {

  private org.slf4j.Logger log;

  public RocksDBLogger(Options options, org.slf4j.Logger log) {
    super(options);
    this.log = log;
  }

  @Override
  protected void log(InfoLogLevel infoLogLevel, String logMsg) {
    switch (infoLogLevel) {
      case INFO_LEVEL:
        log.info(logMsg);
        break;
      case WARN_LEVEL:
        log.warn(logMsg);
        break;
      case DEBUG_LEVEL:
        log.debug(logMsg);
        break;
      case ERROR_LEVEL:
        log.error(logMsg);
        break;
      default:
        log.info("level: {}, log: {}", infoLogLevel, logMsg);
    }
  }
}
