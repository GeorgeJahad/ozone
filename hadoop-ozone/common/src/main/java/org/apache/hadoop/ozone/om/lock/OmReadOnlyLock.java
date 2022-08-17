package org.apache.hadoop.ozone.om.lock;

public class OmReadOnlyLock implements OmLock{
  @Override
  public boolean acquireLock(OzoneManagerLock.Resource resource,
                             String... resources) {
    return false;
  }

  @Override
  public boolean acquireReadLock(OzoneManagerLock.Resource resource,
                                 String... resources) {
    return true;
  }

  @Override
  public boolean acquireWriteLock(OzoneManagerLock.Resource resource,
                                  String... resources) {
    return false;
  }

  @Override
  public boolean acquireMultiUserLock(String firstUser, String secondUser) {
    return false;
  }

  @Override
  public void releaseMultiUserLock(String firstUser, String secondUser) {

  }

  @Override
  public void releaseWriteLock(OzoneManagerLock.Resource resource,
                               String... resources) {

  }

  @Override
  public void releaseReadLock(OzoneManagerLock.Resource resource,
                              String... resources) {

  }

  @Override
  public void releaseLock(OzoneManagerLock.Resource resource,
                          String... resources) {

  }

  @Override
  public int getReadHoldCount(String resourceName) {
    return 0;
  }

  @Override
  public String getReadLockWaitingTimeMsStat() {
    return "";
  }

  @Override
  public long getLongestReadLockWaitingTimeMs() {
    return 0;
  }

  @Override
  public String getReadLockHeldTimeMsStat() {
    return "";
  }

  @Override
  public long getLongestReadLockHeldTimeMs() {
    return 0;
  }

  @Override
  public int getWriteHoldCount(String resourceName) {
    return 0;
  }

  @Override
  public boolean isWriteLockedByCurrentThread(String resourceName) {
    return false;
  }

  @Override
  public String getWriteLockWaitingTimeMsStat() {
    return "";
  }

  @Override
  public long getLongestWriteLockWaitingTimeMs() {
    return 0;
  }

  @Override
  public String getWriteLockHeldTimeMsStat() {
    return "";
  }

  @Override
  public long getLongestWriteLockHeldTimeMs() {
    return 0;
  }

  @Override
  public void cleanup() {

  }

  @Override
  public OMLockMetrics getOMLockMetrics() {
    throw new UnsupportedOperationException("OmReadOnlyLock does not support this operation.");
  }
}
