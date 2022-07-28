package org.apache.hadoop.ozone.om;

public interface OmMReaderMetrics {
  void incNumKeyLookups();

  void incNumKeyLookupFails();

  void incNumListStatus();

  void incNumListStatusFails();

  void incNumGetFileStatus();

  void incNumGetFileStatusFails();

  void incNumLookupFile();

  void incNumLookupFileFails();

  void incNumKeyLists();

  void incNumKeyListFails();

  void incNumGetAcl();
}
