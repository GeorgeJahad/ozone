/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.ozone.om.snapshot;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheLoader;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.om.IOmMetadataReader;
import org.apache.hadoop.ozone.om.OmSnapshot;
import org.apache.hadoop.ozone.om.OmSnapshotManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.KeyInfoWithVolumeContext;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.FILE_NOT_FOUND;
import static org.apache.hadoop.ozone.om.helpers.SnapshotInfo.SnapshotStatus.SNAPSHOT_ACTIVE;

/**
 * Thread-safe custom unbounded LRU cache to manage open snapshot DB instances.
 */
public class SnapshotCache {

  static final Logger LOG = LoggerFactory.getLogger(SnapshotCache.class);

  // Snapshot cache internal hash map.
  // Key:   DB snapshot table key
  // Value: OmSnapshot instance, each holds a DB instance handle inside
  // TODO: Also wrap SoftReference<> around the value?
  private final ConcurrentHashMap<String, ReferenceCounted<IOmMetadataReader>>
      dbMap;

  // Linked hash set that holds OmSnapshot instances whose reference count
  // has reached zero. Those entries are eligible to be evicted and closed.
  // Sorted in last used order.
  // Least-recently-used entry located at the beginning.
  // TODO: Check thread safety. Try ConcurrentHashMultiset ?
  private final LinkedHashSet<IOmMetadataReader>
      pendingEvictionList;
  private final OmSnapshotManager omSnapshotManager;
  private final CacheLoader<String, OmSnapshot> cacheLoader;
  // Soft-limit of the total number of snapshot DB instances allowed to be
  // opened on the OM.
  private final int cacheSizeLimit;

  public SnapshotCache(
      OmSnapshotManager omSnapshotManager,
      CacheLoader<String, OmSnapshot> cacheLoader,
      int cacheSizeLimit) {
    this.dbMap = new ConcurrentHashMap<>();
    this.pendingEvictionList = new LinkedHashSet<>();
    this.omSnapshotManager = omSnapshotManager;
    this.cacheLoader = cacheLoader;
    this.cacheSizeLimit = cacheSizeLimit;
  }

  @VisibleForTesting
  ConcurrentHashMap<String, ReferenceCounted<IOmMetadataReader>> getDbMap() {
    return dbMap;
  }

  @VisibleForTesting
  LinkedHashSet<IOmMetadataReader> getPendingEvictionList() {
    return pendingEvictionList;
  }

  /**
   * @return number of DB instances currently held in cache.
   */
  public int size() {
    return dbMap.size();
  }

  /**
   * Immediately invalidate an entry.
   * @param key DB snapshot table key
   */
  public void invalidate(String key) throws IOException {
    dbMap.computeIfPresent(key, (k, v) -> {
      pendingEvictionList.remove(v.get());
      try {
        (((CacheEntry)v.get()).get()).close();
      } catch (IOException e) {
        throw new IllegalStateException("Failed to close snapshot: " + key, e);
      }
      // Remove the entry from map by returning null
      return null;
    });
  }

  /**
   * Immediately invalidate all entries and close their DB instances in cache.
   */
  public void invalidateAll() {
    Iterator<Map.Entry<String, ReferenceCounted<IOmMetadataReader>>>
        it = dbMap.entrySet().iterator();

    while (it.hasNext()) {
      Map.Entry<String, ReferenceCounted<IOmMetadataReader>> entry = it.next();
      pendingEvictionList.remove(entry.getValue().get());
      OmSnapshot omSnapshot = ((CacheEntry) entry.getValue().get()).get();
      try {
        // TODO: If wrapped with SoftReference<>, omSnapshot could be null?
        omSnapshot.close();
      } catch (IOException e) {
        throw new IllegalStateException("Failed to close snapshot", e);
      }
      it.remove();
    }
  }

  /**
   * State the reason the current thread is getting the OmSnapshot instance.
   */
  public enum Reason {
    FS_API_READ,
    SNAPDIFF_READ,
    DEEP_CLEAN_WRITE,
    GARBAGE_COLLECTION_WRITE
  }


  public ReferenceCounted<IOmMetadataReader> get(String key)
      throws IOException {
    return get(key, false);
  }

  /**
   * Get or load OmSnapshot. Shall be close()d after use.
   * TODO: [SNAPSHOT] Can add reason enum to param list later.
   * @param key snapshot table key
   * @return an OmSnapshot instance, or null on error
   */
  public ReferenceCounted<IOmMetadataReader> get(String key,
      boolean skipActiveCheck) throws IOException {
    // Atomic operation to initialize the OmSnapshot instance (once) if the key
    // does not exist.
    ReferenceCounted<IOmMetadataReader> rcOmSnapshot =
        dbMap.computeIfAbsent(key, k -> {
          LOG.info("Loading snapshot. Table key: {}", k);
          try {
            return new ReferenceCounted<>(new CacheEntry(cacheLoader.load(k)));
          } catch (OMException omEx) {
            // Return null if the snapshot is no longer active
            if (!omEx.getResult().equals(FILE_NOT_FOUND)) {
              throw new IllegalStateException(omEx);
            }
          } catch (IOException ioEx) {
            // Failed to load snapshot DB
            throw new IllegalStateException(ioEx);
          } catch (Exception ex) {
            // Unexpected and unknown exception thrown from CacheLoader#load
            throw new IllegalStateException(ex);
          }
          // Do not put the value in the map on exception
          return null;
        });

    if (rcOmSnapshot == null) {
      // The only exception that would fall through the loader logic above
      // is OMException with FILE_NOT_FOUND.
      throw new OMException("Snapshot table key '" + key + "' not found, "
          + "or the snapshot is no longer active",
          OMException.ResultCodes.FILE_NOT_FOUND);
    }

    // If the snapshot is already loaded in cache, the check inside the loader
    // above is ignored. But we would still want to reject all get()s except
    // when called from SDT (and some) if the snapshot is not active any more.
    if (!omSnapshotManager.isSnapshotStatus(key, SNAPSHOT_ACTIVE) &&
        !skipActiveCheck) {
      throw new OMException("Unable to load snapshot. " +
          "Snapshot with table key '" + key + "' is no longer active",
          FILE_NOT_FOUND);
    }

    // Increment the reference count on the instance.
    rcOmSnapshot.incrementRefCount();

    // Remove instance from clean up list when it exists.
    // TODO: [SNAPSHOT] Check thread safety with release()
    pendingEvictionList.remove(rcOmSnapshot.get());

    // Check if any entries can be cleaned up.
    // At this point, cache size might temporarily exceed cacheSizeLimit
    // even if there are entries that can be evicted, which is fine since it
    // is a soft limit.
    cleanup();

    return rcOmSnapshot;
  }

  /**
   * Release the reference count on the OmSnapshot instance.
   * @param key snapshot table key
   */
  public void release(String key) {
    ReferenceCounted<IOmMetadataReader> rcOmSnapshot = dbMap.get(key);
    Preconditions.checkNotNull(rcOmSnapshot,
        "Key '" + key + "' does not exist in cache");

    if (rcOmSnapshot.decrementRefCount() == 0L) {
      // Eligible to be closed, add it to the list.
      pendingEvictionList.add(rcOmSnapshot.get());
      cleanup();
    }
  }

  /**
   * Evict the reference count on the OmSnapshot instance.
   * @param omSnapshot OmSnapshot
   */
  public void evict(OmSnapshot omSnapshot) {
    final String key = omSnapshot.getSnapshotTableKey();
    ReferenceCounted<IOmMetadataReader> rcOmSnapshot = dbMap.get(key);
    Preconditions.checkNotNull(rcOmSnapshot,
        "Key '" + key + "' does not exist in cache");

    // Eligible to be closed, add it to the list.
    pendingEvictionList.add(rcOmSnapshot.get());
    cleanup();
  }

  /**
   * Alternatively, can release with OmSnapshot instance directly.
   * @param omSnapshot OmSnapshot
   */
  public void release(OmSnapshot omSnapshot) {
    final String key = omSnapshot.getSnapshotTableKey();
    release(key);
  }

  /**
   * If cache size exceeds soft limit, attempt to clean up and close the
   * instances that has zero reference count.
   * TODO: [SNAPSHOT] Add new ozone debug CLI command to trigger this directly.
   */
  private void cleanup() {
    long numEntriesToEvict = (long) dbMap.size() - cacheSizeLimit;
    while (pendingEvictionList.size() > 0 && numEntriesToEvict > 0L) {
      // Get the first instance in the clean up list
      OmSnapshot omSnapshot =
          ((CacheEntry)pendingEvictionList.iterator().next()).get();
      LOG.debug("Evicting OmSnapshot instance with table key {}",
          omSnapshot.getSnapshotTableKey());
      final String key = omSnapshot.getSnapshotTableKey();
      final ReferenceCounted<IOmMetadataReader> result = dbMap.remove(key);
      // Sanity check
      Preconditions.checkState(omSnapshot == ((CacheEntry) result.get()).get(),
          "Cache map entry removal failure. The cache is in an inconsistent "
              + "state. Expected OmSnapshot instance: " + omSnapshot
              + ", actual: " + result.get());

      // Sanity check
      Preconditions.checkState(result.getTotalRefCount() == 0L,
          "Illegal state: OmSnapshot reference count non-zero ("
              + result.getTotalRefCount() + ") but shows up in the "
              + "clean up list");

      pendingEvictionList.remove(result.get());

      // Close the instance, which also closes its DB handle.
      try {
        omSnapshot.close();
      } catch (IOException ex) {
        throw new IllegalStateException("Error while closing snapshot DB", ex);
      }

      --numEntriesToEvict;
    }

    // Print warning message if actual cache size is exceeding the soft limit
    // even after the cleanup procedure above.
    if ((long) dbMap.size() > cacheSizeLimit) {
      LOG.warn("Current snapshot cache size ({}) is exceeding configured "
          + "soft-limit ({}) after possible evictions.",
          dbMap.size(), cacheSizeLimit);

      Preconditions.checkState(pendingEvictionList.size() == 0);
    }
  }

  /**
   * Check cache consistency.
   * @return true if the cache internal structure is consistent to the best of
   * its knowledge, false if found to be inconsistent and details logged.
   */
  public boolean isConsistent() {
    // Using dbMap as the source of truth in this cache, whether dbMap entries
    // are in OM DB's snapshotInfoTable is out of the scope of this check.

    // TODO: [SNAPSHOT]
    // 1. Objects in instancesEligibleForClosure must have ref count equals 0
    // 2. Objects in instancesEligibleForClosure should still be in dbMap
    // 3. instancesEligibleForClosure must be empty if cache size exceeds limit

    return true;
  }

  // The evict() method is the only reason for this class.  The rest
  //  of the methods just delegate to the omSnapshot private field.
  class CacheEntry implements IOmMetadataReader {
    private final OmSnapshot omSnapshot;
    CacheEntry(OmSnapshot omSnapshot) {
      this.omSnapshot = omSnapshot;
    }

    public OmSnapshot get() {
      return omSnapshot;
    }

    @Override
    public void evict() {
      SnapshotCache.this.evict(omSnapshot);
    }

    @Override
    public List<OzoneFileStatus> listStatus(
        OmKeyArgs args, boolean recursive,
        String startKey, long numEntries) throws IOException {
      return omSnapshot.listStatus(args, recursive, startKey, numEntries);
    }

    @Override
    public OmKeyInfo lookupKey(
        OmKeyArgs args) throws IOException {
      return omSnapshot.lookupKey(args);
    }

    @Override
    public KeyInfoWithVolumeContext getKeyInfo(
        OmKeyArgs args,
        boolean assumeS3Context) throws IOException {
      return omSnapshot.getKeyInfo(args, assumeS3Context);
    }

    @Override
    public List<OzoneFileStatus> listStatus(
        OmKeyArgs args, boolean recursive,
        String startKey, long numEntries, boolean allowPartialPrefixes)
        throws IOException {
      return omSnapshot.listStatus(args, recursive, startKey, numEntries,
          allowPartialPrefixes);
    }

    @Override
    public OzoneFileStatus getFileStatus(
        OmKeyArgs args) throws IOException {
      return omSnapshot.getFileStatus(args);
    }

    @Override
    public OmKeyInfo lookupFile(
        OmKeyArgs args) throws IOException {
      return omSnapshot.lookupFile(args);
    }

    @Override
    public List<OmKeyInfo> listKeys(
        String vname, String bname, String startKey, String keyPrefix,
        int maxKeys) throws IOException {
      return omSnapshot.listKeys(vname, bname, startKey, keyPrefix, maxKeys);
    }

    @Override
    public List<OzoneAcl> getAcl(
        OzoneObj obj) throws IOException {
      return omSnapshot.getAcl(obj);
    }

    public String getName() {
      return omSnapshot.getName();
    }
  }

}
