/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.recon.api.handlers;

import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.recon.api.types.DUResponse;
import org.apache.hadoop.ozone.recon.api.types.EntityType;
import org.apache.hadoop.ozone.recon.api.types.NSSummary;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ReconNamespaceSummaryManager;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Class for handling non FSO, Legacy and Object Store buckets.
 */
public class NonFSOBucketHandler extends BucketHandler {

  public NonFSOBucketHandler(
          ReconNamespaceSummaryManager reconNamespaceSummaryManager,
          ReconOMMetadataManager omMetadataManager,
          OzoneStorageContainerManager reconSCM) {
    super(reconNamespaceSummaryManager, omMetadataManager, reconSCM);
  }

  /**
   * Helper function to check if a path is a directory, key, or invalid.
   * @param keyName key name
   * @return DIRECTORY, KEY, or UNKNOWN
   * @throws IOException
   */
  @Override
  public EntityType determineKeyPath(String keyName, long bucketObjectId)
          throws IOException {
    java.nio.file.Path keyPath = Paths.get(keyName);
    Iterator<Path> elements = keyPath.iterator();

    long lastKnownParentId = bucketObjectId;
    OmKeyInfo omKeyInfo;
    while (elements.hasNext()) {
      String fileName = elements.next().toString();

      // For example, /vol1/buck1/a/b/c/d/e/file1.txt
      // Look in the KeyTable, if there is a result, check the keyName
      // if it ends with '/' then we have directory
      // else we have a key
      // otherwise we return null, UNKNOWN
      String dbNodeName = getOmMetadataManager().getOzonePathKey(
              lastKnownParentId, fileName);

      omKeyInfo = getOmMetadataManager().getKeyTable(BucketLayout.LEGACY)
              .getSkipCache(dbNodeName);

      if (omKeyInfo != null) {
        omKeyInfo.setKeyName(keyName);
        if (keyName.endsWith("/")) {
          return EntityType.DIRECTORY;
        } else {
          return EntityType.KEY;
        }
      } else {
        return EntityType.UNKNOWN;
      }
    }
    return EntityType.UNKNOWN;
  }

  // KeyTable's key is in the format of "path/fileName"
  // Make use of RocksDB's order to seek to the prefix and avoid full iteration
  @Override
  public long calculateDUUnderObject(long parentId)
          throws IOException {
    Table keyTable = getOmMetadataManager().getKeyTable(BucketLayout.LEGACY);

    TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>>
            iterator = keyTable.iterator();

    // returns "parentId.path" + "/" + "", path to parent object
    String seekPrefix = getOmMetadataManager()
            .getOzonePathKey(parentId, "");
    iterator.seek(seekPrefix);
    long totalDU = 0L;
    // handle direct keys
    while (iterator.hasNext()) {
      Table.KeyValue<String, OmKeyInfo> kv = iterator.next();
      String dbKey = kv.getKey();
      // since the RocksDB is ordered, seek until the prefix isn't matched
      if (!dbKey.startsWith(seekPrefix)) {
        break;
      }
      OmKeyInfo keyInfo = kv.getValue();
      if (keyInfo != null) {
        totalDU += getKeySizeWithReplication(keyInfo);
      }
    }

    // handle nested keys (DFS)
    NSSummary nsSummary = getReconNamespaceSummaryManager()
            .getNSSummary(parentId);
    // empty bucket
    if (nsSummary == null) {
      return 0;
    }

    Set<Long> subDirIds = nsSummary.getChildDir();
    for (long subDirId: subDirIds) {
      totalDU += calculateDUUnderObject(subDirId);
    }
    return totalDU;
  }

  /**
   * This method handles disk usage of direct keys.
   * @param parentId parent directory/bucket
   * @param withReplica if withReplica is enabled, set sizeWithReplica
   * for each direct key's DU
   * @param listFile if listFile is enabled, append key DU as a subpath
   * @param duData the current DU data
   * @param normalizedPath the normalized path request
   * @return the total DU of all direct keys
   * @throws IOException IOE
   */
  @Override
  public long handleDirectKeys(long parentId, boolean withReplica,
                               boolean listFile,
                               List<DUResponse.DiskUsage> duData,
                               String normalizedPath) throws IOException {

    Table keyTable = getOmMetadataManager().getKeyTable(BucketLayout.LEGACY);
    TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>>
            iterator = keyTable.iterator();

    String seekPrefix = getOmMetadataManager().getOzonePathKey(parentId, "");
    iterator.seek(seekPrefix);

    long keyDataSizeWithReplica = 0L;

    while (iterator.hasNext()) {
      Table.KeyValue<String, OmKeyInfo> kv = iterator.next();
      String dbKey = kv.getKey();

      if (!dbKey.startsWith(seekPrefix)) {
        break;
      }
      OmKeyInfo keyInfo = kv.getValue();
      if (keyInfo != null) {
        DUResponse.DiskUsage diskUsage = new DUResponse.DiskUsage();
        String subpath = buildSubpath(normalizedPath,
                keyInfo.getFileName());
        diskUsage.setSubpath(subpath);
        diskUsage.setKey(true);
        diskUsage.setSize(keyInfo.getDataSize());

        if (withReplica) {
          long keyDU = getKeySizeWithReplication(keyInfo);
          keyDataSizeWithReplica += keyDU;
          diskUsage.setSizeWithReplica(keyDU);
        }
        // list the key as a subpath
        if (listFile) {
          duData.add(diskUsage);
        }
      }
    }
    return keyDataSizeWithReplica;
  }

  /**
   * Given a valid path request for a directory,
   * return the directory object ID.
   * @param names parsed path request in a list of names
   * @return directory object ID
   */
  @Override
  public long getDirObjectId(String[] names) throws IOException {
    return getDirObjectId(names, names.length);
  }

  /**
   * Given a valid path request and a cutoff length where should be iterated
   * up to.
   * return the directory object ID for the object at the cutoff length
   * @param names parsed path request in a list of names
   * @param cutoff cannot be larger than the names' length. If equals,
   *               return the directory object id for the whole path
   * @return directory object ID
   */
  @Override
  public long getDirObjectId(String[] names, int cutoff) throws IOException {
    long dirObjectId = getBucketObjectId(names);
    String dirKey;
    for (int i = 2; i < cutoff; ++i) {
      dirKey = getOmMetadataManager().getOzonePathKey(dirObjectId, names[i]);
      OmKeyInfo dirInfo = getOmMetadataManager()
              .getKeyTable(BucketLayout.LEGACY).getSkipCache(dirKey);
      if (dirInfo.getKeyName().endsWith("/")) {
        dirObjectId = dirInfo.getObjectID();
      }
    }
    return dirObjectId;
  }

}
