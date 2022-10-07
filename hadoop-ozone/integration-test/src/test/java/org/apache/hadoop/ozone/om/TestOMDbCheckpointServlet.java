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

package org.apache.hadoop.ozone.om;

import javax.servlet.ServletContext;
import javax.servlet.ServletOutputStream;
import javax.servlet.WriteListener;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.Principal;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.DBCheckpoint;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.security.UserGroupInformation;

import org.apache.commons.io.FileUtils;

import static org.apache.hadoop.hdds.recon.ReconConfig.ConfigStrings.OZONE_RECON_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_ENABLED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ADMINISTRATORS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ADMINISTRATORS_WILDCARD;
import static org.apache.hadoop.ozone.OzoneConsts.OM_DB_NAME;
import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.OzoneConsts.OM_SNAPSHOT_DIR;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_DB_CHECKPOINT_REQUEST_FLUSH;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_HTTP_AUTH_TYPE;


import org.apache.ozone.test.GenericTestUtils;
import org.junit.After;
import org.junit.Assert;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;
import org.mockito.Matchers;

import static org.apache.hadoop.ozone.om.OMDBCheckpointServlet.fixFile;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Class used for testing the OM DB Checkpoint provider servlet.
 */
public class TestOMDbCheckpointServlet {
  private OzoneConfiguration conf;
  private File tempFile;
  private ServletOutputStream servletOutputStream;
  private MiniOzoneCluster cluster = null;
  private OMMetrics omMetrics = null;
  private HttpServletRequest requestMock = null;
  private HttpServletResponse responseMock = null;
  private OMDBCheckpointServlet omDbCheckpointServletMock = null;
  private File metaDir;

  @Rule
  public Timeout timeout = Timeout.seconds(240);

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();
  /**
   * Create a MiniDFSCluster for testing.
   * <p>
   * Ozone is made active by setting OZONE_ENABLED = true
   *
   * @throws Exception
   */
  @Before
  public void init() throws Exception {
    conf = new OzoneConfiguration();


    tempFile = File.createTempFile("testDoGet_" + System
        .currentTimeMillis(), ".tar.gz");

    FileOutputStream fileOutputStream = new FileOutputStream(tempFile);

    servletOutputStream = new ServletOutputStream() {
      @Override
      public boolean isReady() {
        return true;
      }

      @Override
      public void setWriteListener(WriteListener writeListener) {
      }

      @Override
      public void write(int b) throws IOException {
        fileOutputStream.write(b);
      }
    };
  }

  /**
   * Shutdown MiniDFSCluster.
   */
  @After
  public void shutdown() throws InterruptedException {
    if (cluster != null) {
      cluster.shutdown();
    }
    FileUtils.deleteQuietly(tempFile);
  }

  private void setupCluster() throws Exception {
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(1)
        .build();
    cluster.waitForClusterToBeReady();
    omMetrics = cluster.getOzoneManager().getMetrics();
    metaDir = OMStorage.getOmDbDir(conf);

    omDbCheckpointServletMock =
        mock(OMDBCheckpointServlet.class);

    doCallRealMethod().when(omDbCheckpointServletMock).init();

    requestMock = mock(HttpServletRequest.class);
    // Return current user short name when asked
    when(requestMock.getRemoteUser())
        .thenReturn(UserGroupInformation.getCurrentUser().getShortUserName());
    responseMock = mock(HttpServletResponse.class);

    ServletContext servletContextMock = mock(ServletContext.class);
    when(omDbCheckpointServletMock.getServletContext())
        .thenReturn(servletContextMock);

    when(servletContextMock.getAttribute(OzoneConsts.OM_CONTEXT_ATTRIBUTE))
        .thenReturn(cluster.getOzoneManager());
    when(requestMock.getParameter(OZONE_DB_CHECKPOINT_REQUEST_FLUSH))
        .thenReturn("true");

    doCallRealMethod().when(omDbCheckpointServletMock).doGet(requestMock,
        responseMock);

    doCallRealMethod().when(omDbCheckpointServletMock)
        .returnDBCheckpointToStream(any(), any());
  }

  @Test
  public void testDoGet() throws Exception {
    conf.setBoolean(OZONE_ACL_ENABLED, false);
    conf.set(OZONE_ADMINISTRATORS, OZONE_ADMINISTRATORS_WILDCARD);

    setupCluster();

    final OzoneManager om = cluster.getOzoneManager();

    doCallRealMethod().when(omDbCheckpointServletMock).initialize(
        om.getMetadataManager().getStore(),
        om.getMetrics().getDBCheckpointMetrics(),
        om.getAclsEnabled(),
        om.getOmAdminUsernames(),
        om.getOmAdminGroups(),
        om.isSpnegoEnabled());

    doNothing().when(responseMock).setContentType("application/x-tgz");
    doNothing().when(responseMock).setHeader(Matchers.anyString(),
        Matchers.anyString());

    when(responseMock.getOutputStream()).thenReturn(servletOutputStream);

    omDbCheckpointServletMock.init();
    long initialCheckpointCount =
        omMetrics.getDBCheckpointMetrics().getNumCheckpoints();

    omDbCheckpointServletMock.doGet(requestMock, responseMock);

    Assert.assertTrue(tempFile.length() > 0);
    Assert.assertTrue(
        omMetrics.getDBCheckpointMetrics().
            getLastCheckpointCreationTimeTaken() > 0);
    Assert.assertTrue(
        omMetrics.getDBCheckpointMetrics().
            getLastCheckpointStreamingTimeTaken() > 0);
    Assert.assertTrue(omMetrics.getDBCheckpointMetrics().
        getNumCheckpoints() > initialCheckpointCount);
  }

  @Test
  public void testSpnegoEnabled() throws Exception {
    conf.setBoolean(OZONE_ACL_ENABLED, true);
    conf.set(OZONE_ADMINISTRATORS, "");
    conf.set(OZONE_OM_HTTP_AUTH_TYPE, "kerberos");
    conf.set(OZONE_RECON_KERBEROS_PRINCIPAL_KEY, "recon/host1@REALM");

    setupCluster();

    final OzoneManager om = cluster.getOzoneManager();
    Collection<String> allowedUsers =
            new LinkedHashSet<>(om.getOmAdminUsernames());
    allowedUsers.add("recon");

    doCallRealMethod().when(omDbCheckpointServletMock).initialize(
        om.getMetadataManager().getStore(),
        om.getMetrics().getDBCheckpointMetrics(),
        om.getAclsEnabled(),
        allowedUsers,
        Collections.emptySet(),
        om.isSpnegoEnabled());

    omDbCheckpointServletMock.init();
    omDbCheckpointServletMock.doGet(requestMock, responseMock);

    // Response status should be set to 403 Forbidden since there was no user
    // principal set in the request
    verify(responseMock, times(1)).setStatus(HttpServletResponse.SC_FORBIDDEN);

    // Set the principal to DN in request
    // This should also get denied since only OM and recon
    // users should be granted access to the servlet
    Principal userPrincipalMock = mock(Principal.class);
    when(userPrincipalMock.getName()).thenReturn("dn/localhost@REALM");
    when(requestMock.getUserPrincipal()).thenReturn(userPrincipalMock);

    omDbCheckpointServletMock.doGet(requestMock, responseMock);

    // Verify that the Response status is set to 403 again for DN user.
    verify(responseMock, times(2)).setStatus(HttpServletResponse.SC_FORBIDDEN);

    // Now, set the principal to recon in request
    when(userPrincipalMock.getName()).thenReturn("recon/localhost@REALM");

    when(requestMock.getUserPrincipal()).thenReturn(userPrincipalMock);
    when(responseMock.getOutputStream()).thenReturn(servletOutputStream);

    omDbCheckpointServletMock.doGet(requestMock, responseMock);

    // Recon user should be able to access the servlet and download the
    // snapshot
    Assert.assertTrue(tempFile.length() > 0);
  }

  @Test
  public void testWriteArchiveToStream()
      throws Exception {
    setupCluster();

    OzoneBucket bucket = TestDataUtil.createVolumeAndBucket(cluster);
    TestDataUtil.createKey(bucket, UUID.randomUUID().toString(),
        "content");
    TestDataUtil.createKey(bucket, UUID.randomUUID().toString(),
        "content");

    // this sleep can be removed after this is fixed:
    //  https://issues.apache.org/jira/browse/HDDS-7279
    Thread.sleep(2000);
    String snapshotDirName =
        createSnapshot(bucket.getVolumeName(), bucket.getName());
    String snapshotDirName2 =
        createSnapshot(bucket.getVolumeName(), bucket.getName());

    Path dummyFile = Paths.get(snapshotDirName, "dummyFile");
    Path dummyLink = Paths.get(snapshotDirName2, "dummyFile");
    Files.write(dummyFile, "dummyData".getBytes(StandardCharsets.UTF_8));
    Files.createLink(dummyLink, dummyFile);

    DBCheckpoint dbCheckpoint = cluster.getOzoneManager()
        .getMetadataManager().getStore()
        .getCheckpoint(true);

    FileOutputStream fileOutputStream = new FileOutputStream(tempFile);
    omDbCheckpointServletMock.returnDBCheckpointToStream(dbCheckpoint,
        fileOutputStream);

    String testDirName = folder.newFolder().getAbsolutePath();
    int testDirLength = testDirName.length();
    FileUtil.unTar(tempFile, new File(testDirName));
    Path checkpointLocation = dbCheckpoint.getCheckpointLocation();
    int metaDirLength = metaDir.toString().length();
    String shortCheckpointLocation =
        fixFile(metaDirLength, checkpointLocation);
    Path finalCheckpointLocation =
        Paths.get(testDirName, shortCheckpointLocation);

    Set<String> initialCheckpointSet = new HashSet<>();
    try (Stream<Path> files = Files.list(checkpointLocation)) {
      for (Path file : files.collect(Collectors.toList())) {
        initialCheckpointSet.add(fixFile(metaDirLength, file));
      }
    }
    Set<String> finalCheckpointSet = new HashSet<>();
    try (Stream<Path> files = Files.list(finalCheckpointLocation)) {
      for (Path file : files.collect(Collectors.toList())) {
        finalCheckpointSet.add(fixFile(testDirLength, file));
      }
    }

    String hlString = Paths.get(shortCheckpointLocation,
        "hardLinkFile").toString();
    Assert.assertTrue(finalCheckpointSet.contains(hlString));

    finalCheckpointSet.remove(hlString);


    Assert.assertEquals(initialCheckpointSet, finalCheckpointSet);

  }
  private String createSnapshot(String vname, String bname)
      throws IOException, InterruptedException, TimeoutException {
    final OzoneManager om = cluster.getOzoneManager();
    String snapshotName = UUID.randomUUID().toString();
    OzoneManagerProtocol writeClient = cluster.getRpcClient().getObjectStore()
        .getClientProxy().getOzoneManagerClient();

    writeClient.createSnapshot(vname, bname, snapshotName);
    SnapshotInfo snapshotInfo = om
        .getMetadataManager().getSnapshotInfoTable()
        .get(SnapshotInfo.getTableKey(vname, bname, snapshotName));
    String snapshotDirName = metaDir + OM_KEY_PREFIX +
        OM_SNAPSHOT_DIR + OM_KEY_PREFIX + OM_DB_NAME +
        snapshotInfo.getCheckpointDirName() + OM_KEY_PREFIX;
    GenericTestUtils.waitFor(() -> new File(snapshotDirName).exists(),
        100, 2000);
    return snapshotDirName;
  }

}
