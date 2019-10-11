package com.sdu.rocksdb.utils;

import java.io.Serializable;
import java.util.Objects;

public class StringBasedID implements Serializable {

  private final String keyString;

  public StringBasedID(String keyString) {
    this.keyString = keyString;
  }

  public String getKeyString() {
    return keyString;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    StringBasedID that = (StringBasedID) o;
    return keyString.equals(that.keyString);
  }

  @Override
  public int hashCode() {
    return Objects.hash(keyString);
  }
}
