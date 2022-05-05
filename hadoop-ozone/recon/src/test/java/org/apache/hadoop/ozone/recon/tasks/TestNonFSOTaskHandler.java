/**
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

import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.recon.ReconConstants;
import org.apache.hadoop.ozone.recon.ReconTestInjector;
import org.apache.hadoop.ozone.recon.api.types.NSSummary;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ReconNamespaceSummaryManager;
import org.apache.hadoop.ozone.recon.spi.impl.OzoneManagerServiceProviderImpl;
import org.junit.BeforeClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.ClassRule;
import org.junit.experimental.runners.Enclosed;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.BUCKET_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.VOLUME_TABLE;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.getMockOzoneManagerServiceProvider;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.getTestReconOmMetadataManager;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.initializeNewOmMetadataManager;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.writeDirToOm;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.writeKeyToOm;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Test for NonFSOTaskHandler.
 */
@RunWith(Enclosed.class)
public class TestNonFSOTaskHandler {

  @ClassRule
  public static TemporaryFolder temporaryFolder = new TemporaryFolder();

  private static ReconNamespaceSummaryManager reconNamespaceSummaryManager;
  private static OMMetadataManager omMetadataManager;
  private static ReconOMMetadataManager reconOMMetadataManager;
  private static OzoneManagerServiceProviderImpl ozoneManagerServiceProvider;

  private static NonFSOTaskHandler nonFSOTaskHandler;

  // Object names
  private static final String VOL = "vol";
  private static final String BUCKET_ONE = "bucket1";
  private static final String BUCKET_TWO = "bucket2";
  private static final String KEY_ONE = "file1";
  private static final String KEY_TWO = "file2";
  private static final String KEY_THREE = "dir1/dir2/file3";
  private static final String KEY_FOUR = "file4";
  private static final String KEY_FIVE = "file5";
  private static final String FILE_ONE = "file1";
  private static final String FILE_TWO = "file2";
  private static final String FILE_THREE = "file3";
  private static final String FILE_FOUR = "file4";
  private static final String FILE_FIVE = "file5";
  private static final String DIR_ONE = "dir1";
  private static final String DIR_ONE_RENAME = "dir1_new";
  private static final String DIR_TWO = "dir2";
  private static final String DIR_THREE = "dir3";
  private static final String DIR_FOUR = "dir4";
  private static final String DIR_FIVE = "dir5";

  private static final long BUCKET_ONE_OBJECT_ID = 1L;
  private static final long BUCKET_TWO_OBJECT_ID = 2L;
  private static final long KEY_ONE_OBJECT_ID = 3L;
  private static final long DIR_ONE_OBJECT_ID = 4L;
  private static final long KEY_TWO_OBJECT_ID = 5L;
  private static final long KEY_FOUR_OBJECT_ID = 6L;
  private static final long DIR_TWO_OBJECT_ID = 7L;
  private static final long KEY_THREE_OBJECT_ID = 8L;
  private static final long KEY_FIVE_OBJECT_ID = 9L;
  private static final long DIR_THREE_OBJECT_ID = 10L;
  private static final long DIR_FOUR_OBJECT_ID = 11L;
  private static final long DIR_FIVE_OBJECT_ID = 12L;

  private static final long KEY_ONE_SIZE = 500L;
  private static final long KEY_TWO_OLD_SIZE = 1025L;
  private static final long KEY_TWO_UPDATE_SIZE = 1023L;
  private static final long KEY_THREE_SIZE =
      ReconConstants.MAX_FILE_SIZE_UPPER_BOUND - 100L;
  private static final long KEY_FOUR_SIZE = 2050L;
  private static final long KEY_FIVE_SIZE = 100L;

  private static final long DIR_ONE_SIZE = 100L;

  private static final long DIR_ONE_NEW_SIZE = 200L;

  private static final long DIR_FOUR_SIZE = 400L;

  private static final long DIR_FIVE_SIZE = 500L;

  private static Set<Long> bucketOneAns = new HashSet<>();
  private static Set<Long> bucketTwoAns = new HashSet<>();
  private static Set<Long> dirOneAns = new HashSet<>();
  private static NSSummary nsSummaryForBucket1;
  private static NSSummary nsSummaryForBucket2;

  @BeforeClass
  public static void setUp() throws Exception {

    omMetadataManager = initializeNewOmMetadataManager(
        temporaryFolder.newFolder());
    ozoneManagerServiceProvider =
        getMockOzoneManagerServiceProvider();
    reconOMMetadataManager = getTestReconOmMetadataManager(omMetadataManager,
        temporaryFolder.newFolder());

    ReconTestInjector reconTestInjector =
        new ReconTestInjector.Builder(temporaryFolder)
            .withReconOm(reconOMMetadataManager)
            .withOmServiceProvider(ozoneManagerServiceProvider)
            .withReconSqlDb()
            .withContainerDB()
            .build();
    reconNamespaceSummaryManager =
        reconTestInjector.getInstance(ReconNamespaceSummaryManager.class);

    Table volTableMock = mock(Table.class);
    Table bucketTableMock = mock(Table.class);

    ReconOMMetadataManager mockReconOMMetadataManager = Mockito.spy(reconOMMetadataManager);

    when(volTableMock.getName()).thenReturn(VOLUME_TABLE);
    doReturn(volTableMock).when(mockReconOMMetadataManager).getVolumeTable();
    when(bucketTableMock.getName()).thenReturn(BUCKET_TABLE);
    doReturn(bucketTableMock).when(mockReconOMMetadataManager).getBucketTable();
    OmBucketInfo omBucketInfo = mock(OmBucketInfo.class);
    when(omBucketInfo.getObjectID()).thenReturn(1L);
    when(bucketTableMock.get(any(Object.class))).thenReturn(omBucketInfo);

    nonFSOTaskHandler = new NonFSOTaskHandler(
        reconNamespaceSummaryManager, mockReconOMMetadataManager);
    nonFSOTaskHandler.setBucketLayout(getBucketLayout());

    populateOMDB();
  }

  /**
   * Build a key info for put/update action.
   *
   * @param volume         volume name
   * @param bucket         bucket name
   * @param key            key name
   * @param fileName       file name
   * @param objectID       object ID
   * @param parentObjectId parent object ID
   * @param dataSize       file size
   * @return the KeyInfo
   */
  private static OmKeyInfo buildOmKeyInfo(String volume,
                                          String bucket,
                                          String key,
                                          String fileName,
                                          long objectID,
                                          long parentObjectId,
                                          long dataSize) {
    return new OmKeyInfo.Builder()
        .setBucketName(bucket)
        .setVolumeName(volume)
        .setKeyName(key)
        .setFileName(fileName)
        .setReplicationConfig(
            StandaloneReplicationConfig.getInstance(
                HddsProtos.ReplicationFactor.ONE))
        .setObjectID(objectID)
        .setParentObjectID(parentObjectId)
        .setDataSize(dataSize)
        .build();
  }

  /**
   * Build a key info for delete action.
   *
   * @param volume         volume name
   * @param bucket         bucket name
   * @param key            key name
   * @param fileName       file name
   * @param objectID       object ID
   * @param parentObjectId parent object ID
   * @return the KeyInfo
   */
  private static OmKeyInfo buildOmKeyInfo(String volume,
                                          String bucket,
                                          String key,
                                          String fileName,
                                          long objectID,
                                          long parentObjectId) {
    return new OmKeyInfo.Builder()
        .setBucketName(bucket)
        .setVolumeName(volume)
        .setKeyName(key)
        .setFileName(fileName)
        .setReplicationConfig(
            StandaloneReplicationConfig.getInstance(
                HddsProtos.ReplicationFactor.ONE))
        .setObjectID(objectID)
        .setParentObjectID(parentObjectId)
        .build();
  }

  private static OmDirectoryInfo buildOmDirInfo(String dirName,
                                                long objectId,
                                                long parentObjectId) {
    return new OmDirectoryInfo.Builder()
        .setName(dirName)
        .setObjectID(objectId)
        .setParentObjectID(parentObjectId)
        .build();
  }

  /**
   * Populate OMDB with the following configs.
   * vol
   * /     \
   * bucket1   bucket2
   * /    \      /    \
   * file1  dir1  file4  file5
   * /   \
   * dir2   dir3
   * /
   * file3
   *
   * @throws IOException
   */
  private static void populateOMDB() throws IOException {
    writeKeyToOm(reconOMMetadataManager,
        KEY_ONE,
        BUCKET_ONE,
        VOL,
        FILE_ONE,
        KEY_ONE_OBJECT_ID,
        BUCKET_ONE_OBJECT_ID,
        KEY_ONE_SIZE,
        BucketLayout.LEGACY);
    writeKeyToOm(reconOMMetadataManager,
        KEY_TWO,
        BUCKET_TWO,
        VOL,
        FILE_TWO,
        KEY_TWO_OBJECT_ID,
        BUCKET_TWO_OBJECT_ID,
        KEY_TWO_OLD_SIZE,
        BucketLayout.LEGACY);
    writeKeyToOm(reconOMMetadataManager,
        KEY_THREE,
        BUCKET_ONE,
        VOL,
        FILE_THREE,
        KEY_THREE_OBJECT_ID,
        DIR_TWO_OBJECT_ID,
        KEY_THREE_SIZE,
        BucketLayout.LEGACY);
    writeKeyToOm(reconOMMetadataManager,
        KEY_FOUR,
        BUCKET_TWO,
        VOL,
        FILE_FOUR,
        KEY_FOUR_OBJECT_ID,
        BUCKET_TWO_OBJECT_ID,
        KEY_FOUR_SIZE,
        BucketLayout.LEGACY);
    writeDirToOm(reconOMMetadataManager, DIR_ONE_OBJECT_ID,
        BUCKET_ONE_OBJECT_ID, DIR_ONE);
    writeDirToOm(reconOMMetadataManager, DIR_TWO_OBJECT_ID,
        DIR_ONE_OBJECT_ID, DIR_TWO);
    writeDirToOm(reconOMMetadataManager, DIR_THREE_OBJECT_ID,
        DIR_ONE_OBJECT_ID, DIR_THREE);
  }

  private static BucketLayout getBucketLayout() {
    return BucketLayout.DEFAULT;
  }

  public static class TestReprocess {

    @Before
    public void setUp() throws IOException {
      // write a NSSummary prior to reprocess and verify it got cleaned up after.
      NSSummary staleNSSummary = new NSSummary();
      reconNamespaceSummaryManager.storeNSSummary(-1L, staleNSSummary);

      nonFSOTaskHandler.reprocess(reconOMMetadataManager);

      nsSummaryForBucket1 =
          reconNamespaceSummaryManager.getNSSummary(BUCKET_ONE_OBJECT_ID);
      nsSummaryForBucket2 =
          reconNamespaceSummaryManager.getNSSummary(BUCKET_TWO_OBJECT_ID);
    }

    @Test
    public void testReprocessNSSummaryNotNull() throws IOException {
      assertNull(reconNamespaceSummaryManager.getNSSummary(-1L));
    }

    @Test
    public void testReprocessFileBucketSize() {
      int[] fileDistBucket1 = nsSummaryForBucket1.getFileSizeBucket();
      int[] fileDistBucket2 = nsSummaryForBucket2.getFileSizeBucket();
      assertEquals(ReconConstants.NUM_OF_BINS, fileDistBucket1.length);
      assertEquals(ReconConstants.NUM_OF_BINS, fileDistBucket2.length);

      assertEquals(1, fileDistBucket1[0]);
      for (int i = 1; i < ReconConstants.NUM_OF_BINS; ++i) {
        assertEquals(0, fileDistBucket1[i]);
      }
      assertEquals(1, fileDistBucket2[1]);
      assertEquals(1, fileDistBucket2[2]);
      for (int i = 0; i < ReconConstants.NUM_OF_BINS; ++i) {
        if (i == 1 || i == 2) {
          continue;
        }
        assertEquals(0, fileDistBucket2[i]);
      }
    }

    @Test
    public void testReprocessBucketDirs() {
      // Bucket one has one dir, bucket two has none.
      Set<Long> childDirBucketOne = nsSummaryForBucket1.getChildDir();
      Set<Long> childDirBucketTwo = nsSummaryForBucket2.getChildDir();
      assertEquals(1, childDirBucketOne.size());
      bucketOneAns.clear();
      bucketOneAns.add(DIR_ONE_OBJECT_ID);
      assertEquals(bucketOneAns, childDirBucketOne);
      assertEquals(0, childDirBucketTwo.size());
    }

    @Test
    public void testReprocessDirsUnderDir() throws Exception {

      // Dir 1 has two dir: dir2 and dir3.
      NSSummary nsSummaryInDir1 = reconNamespaceSummaryManager
          .getNSSummary(DIR_ONE_OBJECT_ID);
      assertNotNull(nsSummaryInDir1);
      Set<Long> childDirForDirOne = nsSummaryInDir1.getChildDir();
      assertEquals(2, childDirForDirOne.size());
      dirOneAns.clear();
      dirOneAns.add(DIR_TWO_OBJECT_ID);
      dirOneAns.add(DIR_THREE_OBJECT_ID);
      assertEquals(dirOneAns, childDirForDirOne);

      NSSummary nsSummaryInDir2 = reconNamespaceSummaryManager
          .getNSSummary(DIR_TWO_OBJECT_ID);
      assertEquals(1, nsSummaryInDir2.getNumOfFiles());
      assertEquals(KEY_THREE_SIZE, nsSummaryInDir2.getSizeOfFiles());

      int[] fileDistForDir2 = nsSummaryInDir2.getFileSizeBucket();
      assertEquals(ReconConstants.NUM_OF_BINS, fileDistForDir2.length);
      assertEquals(1, fileDistForDir2[fileDistForDir2.length - 1]);
      for (int i = 0; i < ReconConstants.NUM_OF_BINS - 1; ++i) {
        assertEquals(0, fileDistForDir2[i]);
      }
      assertEquals(0, nsSummaryInDir2.getChildDir().size());

      // bucket should have empty dirName
      assertEquals(0, nsSummaryForBucket1.getDirName().length());
      assertEquals(0, nsSummaryForBucket2.getDirName().length());
      // check dirName is correctly written
      assertEquals(DIR_ONE, nsSummaryInDir1.getDirName());
      assertEquals(DIR_TWO, nsSummaryInDir2.getDirName());
    }
  }

  public static class TestProcess {

    @Before
    public void setUp() throws IOException {
      OMUpdateEventBatch omUpdateEventBatch = processEventBatch();

      nonFSOTaskHandler.reprocess(reconOMMetadataManager);
      nonFSOTaskHandler.process(omUpdateEventBatch);
    }

    protected OMUpdateEventBatch processEventBatch() throws IOException {
      // Events for keyTable change:
      // put file5 under bucket 2
      String omPutKey = BUCKET_TWO_OBJECT_ID + OM_KEY_PREFIX + FILE_FIVE;
      OmKeyInfo omPutKeyInfo = buildOmKeyInfo(VOL, BUCKET_TWO, KEY_FIVE,
          FILE_FIVE, KEY_FIVE_OBJECT_ID, BUCKET_TWO_OBJECT_ID, KEY_FIVE_SIZE);
      OMDBUpdateEvent keyEvent1 = new OMDBUpdateEvent.
          OMUpdateEventBuilder<String, OmKeyInfo>()
          .setKey(omPutKey)
          .setValue(omPutKeyInfo)
          .setTable(omMetadataManager.getKeyTable(getBucketLayout())
              .getName())
          .setAction(OMDBUpdateEvent.OMDBUpdateAction.PUT)
          .build();

      // delete file 1 under bucket 1
      String omDeleteKey = BUCKET_ONE_OBJECT_ID + OM_KEY_PREFIX + FILE_ONE;
      OmKeyInfo omDeleteInfo = buildOmKeyInfo(VOL, BUCKET_ONE, KEY_ONE, FILE_ONE,
          KEY_ONE_OBJECT_ID, BUCKET_ONE_OBJECT_ID);
      OMDBUpdateEvent keyEvent2 = new OMDBUpdateEvent.
          OMUpdateEventBuilder<String, OmKeyInfo>()
          .setKey(omDeleteKey)
          .setValue(omDeleteInfo)
          .setTable(omMetadataManager.getKeyTable(getBucketLayout())
              .getName())
          .setAction(OMDBUpdateEvent.OMDBUpdateAction.DELETE)
          .build();

      // update file 2's size under bucket 2
      String omUpdateKey = BUCKET_TWO_OBJECT_ID + OM_KEY_PREFIX + FILE_TWO;
      OmKeyInfo omOldInfo = buildOmKeyInfo(VOL, BUCKET_TWO, KEY_TWO, FILE_TWO,
          KEY_TWO_OBJECT_ID, BUCKET_TWO_OBJECT_ID, KEY_TWO_OLD_SIZE);
      OmKeyInfo omUpdateInfo = buildOmKeyInfo(VOL, BUCKET_TWO, KEY_TWO, FILE_TWO,
          KEY_TWO_OBJECT_ID, BUCKET_TWO_OBJECT_ID, KEY_TWO_UPDATE_SIZE);
      OMDBUpdateEvent keyEvent3 = new OMDBUpdateEvent.
          OMUpdateEventBuilder<String, OmKeyInfo>()
          .setKey(omUpdateKey)
          .setValue(omUpdateInfo)
          .setOldValue(omOldInfo)
          .setTable(omMetadataManager.getKeyTable(getBucketLayout())
              .getName())
          .setAction(OMDBUpdateEvent.OMDBUpdateAction.UPDATE)
          .build();

      // Events for DirectoryTable change:
      // add dir 4 under bucket 1
      String omDirPutKey1 = BUCKET_ONE_OBJECT_ID + OM_KEY_PREFIX + DIR_FOUR;
      OmKeyInfo omDirPutValue1 = buildOmKeyInfo(VOL, BUCKET_ONE, DIR_FOUR, DIR_FOUR,
          DIR_FOUR_OBJECT_ID, BUCKET_ONE_OBJECT_ID, DIR_FOUR_SIZE);
      OMDBUpdateEvent keyEvent4 = new OMDBUpdateEvent.
          OMUpdateEventBuilder<String, OmKeyInfo>()
          .setKey(omDirPutKey1)
          .setValue(omDirPutValue1)
          .setAction(OMDBUpdateEvent.OMDBUpdateAction.PUT)
          .setTable(omMetadataManager.getKeyTable(getBucketLayout()).getName())
          .build();

      // add dir 5 under bucket 2
      String omDirPutKey2 = BUCKET_TWO_OBJECT_ID + OM_KEY_PREFIX + DIR_FIVE;
      OmKeyInfo omDirPutValue2 = buildOmKeyInfo(VOL, BUCKET_TWO, DIR_FIVE, DIR_FIVE,
          DIR_FIVE_OBJECT_ID, BUCKET_TWO_OBJECT_ID, DIR_FIVE_SIZE);
      OMDBUpdateEvent keyEvent5 = new OMDBUpdateEvent.
          OMUpdateEventBuilder<String, OmKeyInfo>()
          .setKey(omDirPutKey2)
          .setValue(omDirPutValue2)
          .setAction(OMDBUpdateEvent.OMDBUpdateAction.PUT)
          .setTable(omMetadataManager.getKeyTable(getBucketLayout()).getName())
          .build();

      // delete dir 3 under dir 1
      String omDirDeleteKey = DIR_ONE_OBJECT_ID + OM_KEY_PREFIX + DIR_THREE;
      OmKeyInfo omDirDeleteValue = buildOmKeyInfo(VOL, BUCKET_ONE, DIR_THREE, DIR_THREE,
          DIR_THREE_OBJECT_ID, DIR_ONE_OBJECT_ID);
      OMDBUpdateEvent keyEvent6 = new OMDBUpdateEvent.
          OMUpdateEventBuilder<String, OmKeyInfo>()
          .setKey(omDirDeleteKey)
          .setValue(omDirDeleteValue)
          .setAction(OMDBUpdateEvent.OMDBUpdateAction.DELETE)
          .setTable(omMetadataManager.getKeyTable(getBucketLayout()).getName())
          .build();

      // rename dir1
      String omDirUpdateKey = BUCKET_ONE_OBJECT_ID + OM_KEY_PREFIX + DIR_ONE;
      OmKeyInfo omDirOldValue = buildOmKeyInfo(VOL, BUCKET_ONE, DIR_ONE, DIR_ONE,
          DIR_ONE_OBJECT_ID, BUCKET_ONE_OBJECT_ID, DIR_ONE_SIZE);
      OmKeyInfo omDirUpdateValue = buildOmKeyInfo(VOL, BUCKET_ONE, DIR_ONE_RENAME, DIR_ONE_RENAME,
          DIR_ONE_OBJECT_ID, BUCKET_ONE_OBJECT_ID, DIR_ONE_NEW_SIZE);
      OMDBUpdateEvent keyEvent7 = new OMDBUpdateEvent.
          OMUpdateEventBuilder<String, OmKeyInfo>()
          .setKey(omDirUpdateKey)
          .setValue(omDirUpdateValue)
          .setOldValue(omDirOldValue)
          .setAction(OMDBUpdateEvent.OMDBUpdateAction.UPDATE)
          .setTable(omMetadataManager.getKeyTable(getBucketLayout()).getName())
          .build();

      OMUpdateEventBatch omUpdateEventBatch = new OMUpdateEventBatch(
          new ArrayList<OMDBUpdateEvent>() {{
            add(keyEvent1);
            add(keyEvent2);
            add(keyEvent3);
            add(keyEvent4);
            add(keyEvent5);
            add(keyEvent6);
            add(keyEvent7);
          }});

      return omUpdateEventBatch;
    }

    @Test
    public void testProcessUpdateFileSize() throws IOException {
      // file 5 is added under bucket 2, so bucket 2 has 3 keys now
      // file 1 is gone, so bucket 1 is empty now
      // file 2 is updated with new datasize,
      // so file size dist for bucket 2 should be updated
      NSSummary nsSummaryForBucket1 =
          reconNamespaceSummaryManager.getNSSummary(BUCKET_ONE_OBJECT_ID);
      assertNotNull(nsSummaryForBucket1);
      assertEquals(0, nsSummaryForBucket1.getNumOfFiles());

      Set<Long> childDirBucket1 = nsSummaryForBucket1.getChildDir();
      // after put dir4, bucket1 now has two child dirs: dir1 and dir4
      assertEquals(2, childDirBucket1.size());
      bucketOneAns.clear();
      bucketOneAns.add(DIR_ONE_OBJECT_ID);
      bucketOneAns.add(DIR_FOUR_OBJECT_ID);
      assertEquals(bucketOneAns, childDirBucket1);
    }

    @Test
    public void testProcessBucket() throws IOException {
      NSSummary nsSummaryForBucket2 =
          reconNamespaceSummaryManager.getNSSummary(BUCKET_TWO_OBJECT_ID);
      assertNotNull(nsSummaryForBucket2);
      assertEquals(3, nsSummaryForBucket2.getNumOfFiles());
      // key 4 + key 5 + updated key 2
      assertEquals(KEY_FOUR_SIZE + KEY_FIVE_SIZE + KEY_TWO_UPDATE_SIZE,
          nsSummaryForBucket2.getSizeOfFiles());

      int[] fileSizeDist = nsSummaryForBucket2.getFileSizeBucket();
      assertEquals(ReconConstants.NUM_OF_BINS, fileSizeDist.length);
      // 1023L and 100L
      assertEquals(2, fileSizeDist[0]);
      // 2050L
      assertEquals(1, fileSizeDist[2]);
      for (int i = 1; i < ReconConstants.NUM_OF_BINS; ++i) {
        if (i != 2) {
          assertEquals(0, fileSizeDist[i]);
        }
      }

      // after put dir5, bucket 2 now has one dir
      Set<Long> childDirBucket2 = nsSummaryForBucket2.getChildDir();
      assertEquals(1, childDirBucket2.size());
      bucketTwoAns.add(DIR_FIVE_OBJECT_ID);
      assertEquals(bucketTwoAns, childDirBucket2);
    }

    @Test
    public void testProcessDir() throws IOException {

      // after delete dir 3, dir 1 now has only one dir: dir2
      NSSummary nsSummaryForDir1 = reconNamespaceSummaryManager
          .getNSSummary(DIR_ONE_OBJECT_ID);
      assertNotNull(nsSummaryForDir1);
      Set<Long> childDirForDir1 = nsSummaryForDir1.getChildDir();
      assertEquals(1, childDirForDir1.size());
      dirOneAns.clear();
      dirOneAns.add(DIR_TWO_OBJECT_ID);
      assertEquals(dirOneAns, childDirForDir1);

      // after renaming dir1, check its new name
      assertEquals(DIR_ONE_RENAME, nsSummaryForDir1.getDirName());
    }
  }
}
