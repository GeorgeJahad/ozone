package org.apache.hadoop.ozone.om.lock;

import com.google.common.annotations.VisibleForTesting;

/**
 * Interface for OM Metadata locks.
 */
public interface OmLock {
  @Deprecated
  boolean acquireLock(OzoneManagerLock.Resource resource, String... resources);

  boolean acquireReadLock(OzoneManagerLock.Resource resource,
                          String... resources);

  boolean acquireWriteLock(OzoneManagerLock.Resource resource,
                           String... resources);

  boolean acquireMultiUserLock(String firstUser, String secondUser);

  void releaseMultiUserLock(String firstUser, String secondUser);

  void releaseWriteLock(OzoneManagerLock.Resource resource,
                        String... resources);

  void releaseReadLock(OzoneManagerLock.Resource resource, String... resources);

  @Deprecated
  void releaseLock(OzoneManagerLock.Resource resource, String... resources);

  @VisibleForTesting
  int getReadHoldCount(String resourceName);

  @VisibleForTesting
  String getReadLockWaitingTimeMsStat();

  @VisibleForTesting
  long getLongestReadLockWaitingTimeMs();

  @VisibleForTesting
  String getReadLockHeldTimeMsStat();

  @VisibleForTesting
  long getLongestReadLockHeldTimeMs();

  @VisibleForTesting
  int getWriteHoldCount(String resourceName);

  @VisibleForTesting
  boolean isWriteLockedByCurrentThread(String resourceName);

  @VisibleForTesting
  String getWriteLockWaitingTimeMsStat();

  @VisibleForTesting
  long getLongestWriteLockWaitingTimeMs();

  @VisibleForTesting
  String getWriteLockHeldTimeMsStat();

  @VisibleForTesting
  long getLongestWriteLockHeldTimeMs();

  void cleanup();

  OMLockMetrics getOMLockMetrics();
}
