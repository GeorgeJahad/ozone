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
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.DBCheckpoint;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.security.UserGroupInformation;

import org.apache.commons.io.FileUtils;

import static org.apache.hadoop.hdds.recon.ReconConfig.ConfigStrings.OZONE_RECON_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.hdds.utils.HddsServerUtil.writeDBCheckpointToStream;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_ENABLED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ADMINISTRATORS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ADMINISTRATORS_WILDCARD;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_DB_CHECKPOINT_REQUEST_FLUSH;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_HTTP_AUTH_TYPE;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;
import org.mockito.Matchers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
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
        .currentTimeMillis(), ".tar");

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

    doNothing().when(responseMock).setContentType("application/x-tar");
    doNothing().when(responseMock).setHeader(Matchers.anyString(),
        Matchers.anyString());

    when(responseMock.getOutputStream()).thenReturn(servletOutputStream);

    omDbCheckpointServletMock.init();
    long initialCheckpointCount =
        omMetrics.getDBCheckpointMetrics().getNumCheckpoints();

    omDbCheckpointServletMock.doGet(requestMock, responseMock);

    assertTrue(tempFile.length() > 0);
    assertTrue(
        omMetrics.getDBCheckpointMetrics().
            getLastCheckpointCreationTimeTaken() > 0);
    assertTrue(
        omMetrics.getDBCheckpointMetrics().
            getLastCheckpointStreamingTimeTaken() > 0);
    assertTrue(omMetrics.getDBCheckpointMetrics().
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
    assertTrue(tempFile.length() > 0);
  }

  @Test
  public void testWriteCheckpointToOutputStream() throws Exception {

    String testDirName = folder.newFolder().getAbsolutePath();
    File checkpoint = new File(testDirName, "checkpoint");
    checkpoint.mkdir();
    File file = new File(checkpoint, "temp1.txt");
    OutputStreamWriter writer = new OutputStreamWriter(
        new FileOutputStream(file), StandardCharsets.UTF_8);
    writer.write("Test data 1");
    writer.close();

    file = new File(checkpoint, "/temp2.txt");
    writer = new OutputStreamWriter(
        new FileOutputStream(file), StandardCharsets.UTF_8);
    writer.write("Test data 2");
    writer.close();

    File outputFile =
        new File(Paths.get(testDirName, "output_file.tar").toString());
    TestDBCheckpoint dbCheckpoint = new TestDBCheckpoint(checkpoint.toPath());
    writeDBCheckpointToStream(dbCheckpoint,
        new FileOutputStream(outputFile), new ArrayList<>());
    assertNotNull(outputFile);
  }

  @Test
  public void testWriteIncrementalCheckpointToOutputStream() throws Exception {

    File testRootDir = folder.newFolder();
    File testDir = new File(testRootDir, "checkpoint");
    assertTrue(testDir.mkdir());

    File file1 = new File(testDir, "1.sst");
    OutputStreamWriter writer = new OutputStreamWriter(
        new FileOutputStream(file1), StandardCharsets.UTF_8);
    writer.write("Test data 1");
    writer.close();

    File file2 = new File(testDir, "2.sst");
    writer = new OutputStreamWriter(
        new FileOutputStream(file2), StandardCharsets.UTF_8);
    writer.write("Test data 2");
    writer.close();

    File file3 = new File(testDir, "3.sst");
    writer = new OutputStreamWriter(
        new FileOutputStream(file3), StandardCharsets.UTF_8);
    writer.write("Test data 3");
    writer.close();

    File outputFile = new File(testRootDir, "output_file.tar");
    TestDBCheckpoint dbCheckpoint = new TestDBCheckpoint(
        testDir.toPath());

    // exclude file2 as existing SST
    FileOutputStream outputStream = new FileOutputStream(outputFile);
    List<String> receivedSstList = new ArrayList<>();
    receivedSstList.add(file2.getName());
    List<String> excludedSstList = writeDBCheckpointToStream(dbCheckpoint,
        outputStream, receivedSstList);
    assertEquals(file2.getName(), excludedSstList.get(0));
    assertTrue(outputFile.exists());

    // the files in outputStream should only contain file1 and file3
    File untarredDbDir = new File(testRootDir, "untar.db");
    FileUtil.unTar(outputFile, untarredDbDir);
    String[] untarredfiles = untarredDbDir.list();
    List<String> expectedFiles = new ArrayList<>();
    expectedFiles.add(file1.getName());
    expectedFiles.add(file3.getName());
    assertNotNull(untarredfiles);
    assertEquals(new HashSet<>(expectedFiles),
        Arrays.stream(untarredfiles).collect(Collectors.toSet()));
  }
}

class TestDBCheckpoint implements DBCheckpoint {

  private final Path checkpointFile;

  TestDBCheckpoint(Path checkpointFile) {
    this.checkpointFile = checkpointFile;
  }

  @Override
  public Path getCheckpointLocation() {
    return checkpointFile;
  }

  @Override
  public long getCheckpointTimestamp() {
    return 0;
  }

  @Override
  public long getLatestSequenceNumber() {
    return 0;
  }

  @Override
  public long checkpointCreationTimeTaken() {
    return 0;
  }

  @Override
  public void cleanupCheckpoint() throws IOException {
    FileUtils.deleteDirectory(checkpointFile.toFile());
  }
}