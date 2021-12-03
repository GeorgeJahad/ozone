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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hadoop.ozone.om;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.HddsWhiteboxTestUtils;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdds.scm.protocol.ScmBlockLocationProtocol;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.hdds.server.ServerUtils;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.OpenKeySession;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.om.request.TestOMRequestUtils;
import org.apache.hadoop.ozone.security.OzoneBlockTokenSecretManager;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.hadoop.hdds.utils.db.DBConfigFromFile;

import org.apache.commons.lang3.RandomStringUtils;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_CONTAINER_REPORT_INTERVAL;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_BLOCK_DELETING_SERVICE_INTERVAL;

import org.junit.After;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

/**
 * Test Key Deleting Service.
 * <p>
 * This test does the following things.
 * <p>
 * 1. Creates a bunch of keys. 2. Then executes delete key directly using
 * Metadata Manager. 3. Waits for a while for the KeyDeleting Service to pick up
 * and call into SCM. 4. Confirms that calls have been successful.
 */
public class TestKeyDeletingService {
  @Rule
  public TemporaryFolder folder = new TemporaryFolder();
  private static OzoneManagerProtocol writeClient;
  private static OzoneManager om;
  private static ScmBlockLocationProtocol blockClient;
  private static StorageContainerLocationProtocol containerClient;

  private OzoneConfiguration createConfAndInitValues() throws IOException {
    OzoneConfiguration conf = new OzoneConfiguration();
    File newFolder = folder.newFolder();
    if (!newFolder.exists()) {
      Assert.assertTrue(newFolder.mkdirs());
    }
    System.setProperty(DBConfigFromFile.CONFIG_DIR, "/");
    ServerUtils.setOzoneMetaDirPath(conf, newFolder.toString());
    conf.setTimeDuration(OZONE_BLOCK_DELETING_SERVICE_INTERVAL, 100,
        TimeUnit.MILLISECONDS);
    conf.setTimeDuration(HDDS_CONTAINER_REPORT_INTERVAL, 200,
        TimeUnit.MILLISECONDS);
    conf.setQuietMode(false);

    return conf;
  }

  @After
  public void cleanup() throws Exception {
    om.stop();
  }

  /**
   * In this test, we create a bunch of keys and delete them. Then we start the
   * KeyDeletingService and pass a SCMClient which does not fail. We make sure
   * that all the keys that we deleted is picked up and deleted by
   * OzoneManager.
   *
   * @throws IOException - on Failure.
   */

  @Test(timeout = 30000)
  public void checkIfDeleteServiceisDeletingKeys()
      throws IOException, TimeoutException, InterruptedException,
      AuthenticationException {
    OzoneConfiguration conf = createConfAndInitValues();
    containerClient = Mockito.mock(StorageContainerLocationProtocol.class);
    blockClient = new ScmBlockLocationTestingClient(null, null, 0);
    KeyManager keyManager = initKeyManager(conf);

    final int keyCount = 100;
    createAndDeleteKeys(keyManager, keyCount, 1);
    KeyDeletingService keyDeletingService =
        (KeyDeletingService) keyManager.getDeletingService();
    GenericTestUtils.waitFor(
        () -> keyDeletingService.getDeletedKeyCount().get() >= keyCount,
        1000, 10000);
    Assert.assertTrue(keyDeletingService.getRunCount().get() > 1);
    Assert.assertEquals(
        keyManager.getPendingDeletionKeys(Integer.MAX_VALUE).size(), 0);
  }

  @Test(timeout = 30000)
  public void checkIfDeleteServiceWithFailingSCM()
      throws IOException, TimeoutException, InterruptedException,
      AuthenticationException {
    OzoneConfiguration conf = createConfAndInitValues();
    containerClient = Mockito.mock(StorageContainerLocationProtocol.class);
    blockClient = new ScmBlockLocationTestingClient(null, null, 1);
    KeyManager keyManager = initKeyManager(conf);

    final int keyCount = 100;
    createAndDeleteKeys(keyManager, keyCount, 1);
    KeyDeletingService keyDeletingService =
        (KeyDeletingService) keyManager.getDeletingService();
    Assert.assertEquals(
        keyManager.getPendingDeletionKeys(Integer.MAX_VALUE).size(), keyCount);
    // Make sure that we have run the background thread 5 times more
    GenericTestUtils.waitFor(
        () -> keyDeletingService.getRunCount().get() >= 5,
        100, 1000);
    // Since SCM calls are failing, deletedKeyCount should be zero.
    Assert.assertEquals(keyDeletingService.getDeletedKeyCount().get(), 0);
    Assert.assertEquals(
        keyManager.getPendingDeletionKeys(Integer.MAX_VALUE).size(), keyCount);
  }

  @Test(timeout = 30000)
  public void checkDeletionForEmptyKey()
      throws IOException, TimeoutException, InterruptedException,
      AuthenticationException {
    OzoneConfiguration conf = createConfAndInitValues();
    containerClient = Mockito.mock(StorageContainerLocationProtocol.class);
    blockClient = new ScmBlockLocationTestingClient(null, null, 1);
    KeyManager keyManager = initKeyManager(conf);

    final int keyCount = 100;
    createAndDeleteKeys(keyManager, keyCount, 0);
    KeyDeletingService keyDeletingService =
        (KeyDeletingService) keyManager.getDeletingService();

    // Since empty keys are directly deleted from db there should be no
    // pending deletion keys. Also deletedKeyCount should be zero.
    Assert.assertEquals(
        keyManager.getPendingDeletionKeys(Integer.MAX_VALUE).size(), 0);
    // Make sure that we have run the background thread 2 times or more
    GenericTestUtils.waitFor(
        () -> keyDeletingService.getRunCount().get() >= 2,
        100, 1000);
    Assert.assertEquals(keyDeletingService.getDeletedKeyCount().get(), 0);
  }

  private void createAndDeleteKeys(KeyManager keyManager, int keyCount,
      int numBlocks) throws IOException {
    for (int x = 0; x < keyCount; x++) {
      String volumeName = String.format("volume%s",
          RandomStringUtils.randomAlphanumeric(5));
      String bucketName = String.format("bucket%s",
          RandomStringUtils.randomAlphanumeric(5));
      String keyName = String.format("key%s",
          RandomStringUtils.randomAlphanumeric(5));
      String volumeBytes =
          keyManager.getMetadataManager().getVolumeKey(volumeName);
      String bucketBytes =
          keyManager.getMetadataManager().getBucketKey(volumeName, bucketName);
      // cheat here, just create a volume and bucket entry so that we can
      // create the keys, we put the same data for key and value since the
      // system does not decode the object
      TestOMRequestUtils.addVolumeToOM(keyManager.getMetadataManager(),
          OmVolumeArgs.newBuilder()
              .setOwnerName("o")
              .setAdminName("a")
              .setVolume(volumeName)
              .build());

      TestOMRequestUtils.addBucketToOM(keyManager.getMetadataManager(),
          OmBucketInfo.newBuilder().setVolumeName(volumeName)
              .setBucketName(bucketName)
              .build());

      OmKeyArgs arg =
          new OmKeyArgs.Builder()
              .setVolumeName(volumeName)
              .setBucketName(bucketName)
              .setKeyName(keyName)
              .setAcls(Collections.emptyList())
              .setReplicationConfig(new StandaloneReplicationConfig(
                  HddsProtos.ReplicationFactor.ONE))
              .setLocationInfoList(new ArrayList<>())
              .build();
      //Open, Commit and Delete the Keys in the Key Manager.
      OpenKeySession session = writeClient.openKey(arg);
      for (int i = 0; i < numBlocks; i++) {
        arg.addLocationInfo(
            writeClient.allocateBlock(arg, session.getId(), new ExcludeList()));
      }
      writeClient.commitKey(arg, session.getId());
      writeClient.deleteKey(arg);
    }
  }
  private KeyManager initKeyManager(OzoneConfiguration conf)
      throws AuthenticationException, IOException {
    conf.set(ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY, "127.0.0.1:0");
    DefaultMetricsSystem.setMiniClusterMode(true);
    OMStorage omStorage = new OMStorage(conf);
    omStorage.setClusterId("omtest");
    omStorage.setOmId("omtest");
    omStorage.initialize();
    System.out.println("gbj3");
    OzoneManager.setTestSecureOmFlag(true);
    om = OzoneManager.createOm(conf, OzoneManager.StartupOption.REGUALR, containerClient, blockClient);
    System.out.println("gbj4");

    KeyManager keyManager = (KeyManagerImpl) HddsWhiteboxTestUtils.getInternalState(om, "keyManager");
    ScmClient scmClient = new ScmClient(blockClient, containerClient);
    HddsWhiteboxTestUtils.setInternalState(keyManager,
        "scmClient", scmClient);
    HddsWhiteboxTestUtils.setInternalState(keyManager,
        "secretManager", Mockito.mock(OzoneBlockTokenSecretManager.class));
    System.out.println("gbj5");

    om.start();
    writeClient = OzoneClientFactory.getRpcClient(conf).getObjectStore().getClientProxy().getOzoneManagerClient();
    return keyManager;
  }
}