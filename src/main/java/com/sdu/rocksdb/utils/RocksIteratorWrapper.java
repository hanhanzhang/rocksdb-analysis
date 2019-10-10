package com.sdu.rocksdb.utils;

import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.RocksIteratorInterface;

/**
 * @author hanhan.zhang
 * */
public class RocksIteratorWrapper implements RocksIteratorInterface {

  private final RocksIterator iterator;

  public RocksIteratorWrapper(RocksIterator iterator) {
    this.iterator = iterator;
  }

  @Override
  public boolean isValid() {
    status();
    return iterator.isValid();
  }

  @Override
  public void seekToFirst() {
    status();
    iterator.seekToFirst();
  }

  @Override
  public void seekToLast() {
    status();
    iterator.seekToLast();
  }

  @Override
  public void seek(byte[] target) {
    status();
    iterator.seek(target);
  }

  @Override
  public void seekForPrev(byte[] target) {
    status();
    iterator.seekForPrev(target);
  }

  @Override
  public void next() {
    status();
    iterator.next();
  }

  @Override
  public void prev() {
    status();
    iterator.next();
  }

  @Override
  public void status() {
    try {
      iterator.status();
    } catch (RocksDBException ex) {
      throw new RuntimeException("Internal exception found in RocksDB", ex);
    }
  }

  public byte[] key() {
    return iterator.key();
  }

  public byte[] value() {
    return iterator.value();
  }

}
