/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.recon.tasks;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.recon.ReconUtils;
import org.apache.hadoop.ozone.recon.api.types.NSSummary;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ReconNamespaceSummaryManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.IOException;

/**
 * Task to query data from OMDB and write into Recon RocksDB.
 * Reprocess() will take a snapshots on OMDB, and iterate the keyTable and
 * dirTable to write all information to RocksDB.
 *
 * For FSO-enabled keyTable (fileTable), we need to fetch the parent object
 * (bucket or directory), increment its numOfKeys by 1, increase its sizeOfKeys
 * by the file data size, and update the file size distribution bin accordingly.
 *
 * For dirTable, we need to fetch the parent object (bucket or directory),
 * add the current directory's objectID to the parent object's childDir field.
 *
 * Process() will write all OMDB updates to RocksDB.
 * The write logic is the same as above. For update action, we will treat it as
 * delete old value first, and write updated value then.
 */
public abstract class NSSummaryTask implements ReconOmTask {
  private static final Logger LOG =
          LoggerFactory.getLogger(NSSummaryTask.class);

  private ReconNamespaceSummaryManager reconNamespaceSummaryManager;

  private ReconOMMetadataManager omMetadataManager;

  @Inject
  public NSSummaryTask(ReconNamespaceSummaryManager
                                 reconNamespaceSummaryManager,
                       ReconOMMetadataManager omMetadataManager) {
    this.reconNamespaceSummaryManager = reconNamespaceSummaryManager;
    this.omMetadataManager = omMetadataManager;
  }

  public ReconNamespaceSummaryManager getReconNamespaceSummaryManager() {
    return reconNamespaceSummaryManager;
  }

  public ReconOMMetadataManager getReconOMMetadataManager() {
    return omMetadataManager;
  }

  public abstract String getTaskName();

  public abstract Pair<String, Boolean> process(OMUpdateEventBatch events);

  public abstract Pair<String, Boolean> reprocess(
      OMMetadataManager omMetadataManager);

  protected void writeOmKeyInfoOnNamespaceDB(OmKeyInfo keyInfo)
          throws IOException {
    long parentObjectId = keyInfo.getParentObjectID();
    NSSummary nsSummary = reconNamespaceSummaryManager
            .getNSSummary(parentObjectId);
    if (nsSummary == null) {
      nsSummary = new NSSummary();
    }
    int numOfFile = nsSummary.getNumOfFiles();
    long sizeOfFile = nsSummary.getSizeOfFiles();
    int[] fileBucket = nsSummary.getFileSizeBucket();
    nsSummary.setNumOfFiles(numOfFile + 1);
    long dataSize = keyInfo.getDataSize();
    nsSummary.setSizeOfFiles(sizeOfFile + dataSize);
    int binIndex = ReconUtils.getBinIndex(dataSize);

    ++fileBucket[binIndex];
    nsSummary.setFileSizeBucket(fileBucket);
    reconNamespaceSummaryManager.storeNSSummary(parentObjectId, nsSummary);
  }

  protected void writeOmDirectoryInfoOnNamespaceDB(
      OmDirectoryInfo directoryInfo)
          throws IOException {
    long parentObjectId = directoryInfo.getParentObjectID();
    long objectId = directoryInfo.getObjectID();
    // write the dir name to the current directory
    String dirName = directoryInfo.getName();
    NSSummary curNSSummary =
            reconNamespaceSummaryManager.getNSSummary(objectId);
    if (curNSSummary == null) {
      curNSSummary = new NSSummary();
    }
    curNSSummary.setDirName(dirName);
    reconNamespaceSummaryManager.storeNSSummary(objectId, curNSSummary);

    // write the child dir list to the parent directory
    NSSummary nsSummary = reconNamespaceSummaryManager
            .getNSSummary(parentObjectId);
    if (nsSummary == null) {
      nsSummary = new NSSummary();
    }
    nsSummary.addChildDir(objectId);
    reconNamespaceSummaryManager.storeNSSummary(parentObjectId, nsSummary);
  }

  protected void deleteOmKeyInfoOnNamespaceDB(OmKeyInfo keyInfo)
          throws IOException {
    long parentObjectId = keyInfo.getParentObjectID();
    NSSummary nsSummary = reconNamespaceSummaryManager
            .getNSSummary(parentObjectId);

    // Just in case the OmKeyInfo isn't correctly written.
    if (nsSummary == null) {
      LOG.error("The namespace table is not correctly populated.");
      return;
    }
    int numOfFile = nsSummary.getNumOfFiles();
    long sizeOfFile = nsSummary.getSizeOfFiles();
    int[] fileBucket = nsSummary.getFileSizeBucket();

    long dataSize = keyInfo.getDataSize();
    int binIndex = ReconUtils.getBinIndex(dataSize);

    // decrement count, data size, and bucket count
    // even if there's no direct key, we still keep the entry because
    // we still need children dir IDs info
    nsSummary.setNumOfFiles(numOfFile - 1);
    nsSummary.setSizeOfFiles(sizeOfFile - dataSize);
    --fileBucket[binIndex];
    nsSummary.setFileSizeBucket(fileBucket);
    reconNamespaceSummaryManager.storeNSSummary(parentObjectId, nsSummary);
  }

  protected void deleteOmDirectoryInfoOnNamespaceDB(
      OmDirectoryInfo directoryInfo)
          throws IOException {
    long parentObjectId = directoryInfo.getParentObjectID();
    long objectId = directoryInfo.getObjectID();
    NSSummary nsSummary = reconNamespaceSummaryManager
            .getNSSummary(parentObjectId);

    // Just in case the OmDirectoryInfo isn't correctly written.
    if (nsSummary == null) {
      LOG.error("The namespace table is not correctly populated.");
      return;
    }

    nsSummary.removeChildDir(objectId);
    reconNamespaceSummaryManager.storeNSSummary(parentObjectId, nsSummary);
  }
}

