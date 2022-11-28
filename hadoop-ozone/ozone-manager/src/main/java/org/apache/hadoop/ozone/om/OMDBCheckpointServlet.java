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

package org.apache.hadoop.ozone.om;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.compress.archivers.ArchiveOutputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorOutputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.recon.ReconConfig;
import org.apache.hadoop.hdds.server.ServerUtils;
import org.apache.hadoop.hdds.utils.DBCheckpointServlet;
import org.apache.hadoop.hdds.utils.db.DBCheckpoint;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hadoop.ozone.OzoneConsts.OM_CHECKPOINT_DIR;
import static org.apache.hadoop.ozone.OzoneConsts.OM_SNAPSHOT_DIR;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_DB_CHECKPOINT_INCLUDE_SNAPSHOT_DATA;
import static org.apache.hadoop.ozone.om.OmSnapshotManager.createHardLinkList;
import static org.apache.hadoop.ozone.om.OmSnapshotManager.getSnapshotPath;
import static org.apache.hadoop.ozone.om.OmSnapshotManager.truncateFileName;

/**
 * Provides the current checkpoint Snapshot of the OM DB. (tar.gz)
 *
 * When Ozone ACL is enabled (`ozone.acl.enabled`=`true`), only users/principals
 * configured in `ozone.administrator` (along with the user that starts OM,
 * which automatically becomes an Ozone administrator but not necessarily in
 * the config) are allowed to access this endpoint.
 *
 * If Kerberos is enabled, the principal should be appended to
 * `ozone.administrator`, e.g. `scm/scm@EXAMPLE.COM`
 * If Kerberos is not enabled, simply append the login user name to
 * `ozone.administrator`, e.g. `scm`
 */
public class OMDBCheckpointServlet extends DBCheckpointServlet {

  private static final Logger LOG =
      LoggerFactory.getLogger(OMDBCheckpointServlet.class);
  private static final long serialVersionUID = 1L;
  private static final String DURATION_TO_WAIT_FOR_DIRECTORY = "PT10S";

  @Override
  public void init() throws ServletException {

    OzoneManager om = (OzoneManager) getServletContext()
        .getAttribute(OzoneConsts.OM_CONTEXT_ATTRIBUTE);

    if (om == null) {
      LOG.error("Unable to initialize OMDBCheckpointServlet. OM is null");
      return;
    }

    OzoneConfiguration conf = getConf();
    // Only Ozone Admins and Recon are allowed
    Collection<String> allowedUsers =
            new LinkedHashSet<>(om.getOmAdminUsernames());
    Collection<String> allowedGroups = om.getOmAdminGroups();
    ReconConfig reconConfig = conf.getObject(ReconConfig.class);
    String reconPrincipal = reconConfig.getKerberosPrincipal();
    if (!reconPrincipal.isEmpty()) {
      UserGroupInformation ugi =
          UserGroupInformation.createRemoteUser(reconPrincipal);
      allowedUsers.add(ugi.getShortUserName());
    }

    initialize(om.getMetadataManager().getStore(),
        om.getMetrics().getDBCheckpointMetrics(),
        om.getAclsEnabled(),
        allowedUsers,
        allowedGroups,
        om.isSpnegoEnabled());
  }

  @Override
  public void writeDbDataToStream(DBCheckpoint checkpoint,
                                  HttpServletRequest request,
                                  OutputStream destination)
      throws IOException, InterruptedException, CompressorException {
    // Map of inodes to path
    HashMap<Object, Path> copyFiles = new HashMap<>();
    // Map of link to path
    HashMap<Path, Path> hardLinkFiles = new HashMap<>();

    getFilesForArchive(checkpoint, copyFiles, hardLinkFiles,
        includeSnapshotData(request));

    try (CompressorOutputStream gzippedOut = new CompressorStreamFactory()
        .createCompressorOutputStream(CompressorStreamFactory.GZIP, destination);
         TarArchiveOutputStream archiveOutputStream = 
             new TarArchiveOutputStream(gzippedOut)
    ) {
      archiveOutputStream.setLongFileMode(TarArchiveOutputStream.LONGFILE_POSIX);
      writeFilesToArchive(copyFiles, hardLinkFiles, archiveOutputStream);
    } catch (CompressorException e) {
      throw new IOException(
          "Can't compress the checkpoint: " +
              checkpoint.getCheckpointLocation(), e);
    }
  }

  private void getFilesForArchive(DBCheckpoint checkpoint,
                                  Map<Object, Path> copyFiles,
                                  Map<Path, Path> hardLinkFiles,
                                  boolean includeSnapshotData)
      throws IOException, InterruptedException {

    // Get the active fs files
    Path dir = checkpoint.getCheckpointLocation();
    processDir(dir, copyFiles, hardLinkFiles);

    if (!includeSnapshotData) {
      return;
    }

    // Get the snapshot files
    waitForSnapshotDirs(checkpoint);
    Path snapshotDir = Paths.get(OMStorage.getOmDbDir(getConf()).toString(),
        OM_SNAPSHOT_DIR);
    processDir(snapshotDir, copyFiles, hardLinkFiles);
  }

  // The snapshotInfo table may contain a snapshot that
  //  doesn't yet exist on the fs, so wait a few seconds for it
  private void waitForSnapshotDirs(DBCheckpoint checkpoint)
      throws IOException, InterruptedException {

    OzoneConfiguration conf = getConf();

    // get snapshotInfo entries
    OmMetadataManagerImpl checkpointMetadataManager =
        OmMetadataManagerImpl.createCheckpointMetadataManager(
            conf, checkpoint);
    try (TableIterator<String, ? extends Table.KeyValue<String, SnapshotInfo>>
        iterator = checkpointMetadataManager
        .getSnapshotInfoTable().iterator()) {

      // for each entry, wait for corresponding directory
      while (iterator.hasNext()) {
        Table.KeyValue<String, SnapshotInfo> entry = iterator.next();
        Path path = Paths.get(getSnapshotPath(conf, entry.getValue()));
        waitForDirToExist(path);
      }
    }
  }

  private void waitForDirToExist(Path dir)
      throws IOException, InterruptedException {
    long endTime = System.currentTimeMillis() +
        Duration.parse(DURATION_TO_WAIT_FOR_DIRECTORY).toMillis();
    while (!dir.toFile().exists()) {
      Thread.sleep(100);
      if (System.currentTimeMillis() > endTime) {
        break;
      }
    }
    if (System.currentTimeMillis() > endTime) {
      throw new IOException("snapshot dir doesn't exist: " + dir);
    }
  }

  private void processDir(Path dir, Map<Object, Path> copyFiles,
                          Map<Path, Path> hardLinkFiles)
      throws IOException {
    try (Stream<Path> files = Files.list(dir)) {
      for (Path file : files.collect(Collectors.toList())) {
        if (file.toFile().isDirectory()) {
          processDir(file, copyFiles, hardLinkFiles);
        } else {
          processFile(file, copyFiles, hardLinkFiles);
        }
      }
    }
  }

  private void processFile(Path file, Map<Object, Path> copyFiles,
                           Map<Path, Path> hardLinkFiles) throws IOException {
    // get the inode
    Object key = OmSnapshotManager.getINode(file);
    // If we already have the inode, store as hard link
    if (copyFiles.containsKey(key)) {
      hardLinkFiles.put(file, copyFiles.get(key));
    } else {
      copyFiles.put(key, file);
    }
  }

  // returns value of http request parameter
  private boolean includeSnapshotData(HttpServletRequest request) {
    String includeParam =
        request.getParameter(OZONE_DB_CHECKPOINT_INCLUDE_SNAPSHOT_DATA);
    if (StringUtils.isNotEmpty(includeParam)) {
      return Boolean.parseBoolean(includeParam);
    }
    return false;
  }

  @SuppressFBWarnings({"NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE"})
  private void writeFilesToArchive(HashMap<Object, Path> copyFiles,
                         HashMap<Path, Path> hardLinkFiles,
                         ArchiveOutputStream archiveOutputStream)
      throws IOException {

    File metaDirPath = ServerUtils.getOzoneMetaDirPath(getConf());
    int truncateLength = metaDirPath.toString().length() + 1;

    // Go through each of the files to be copied and add to archive
    for (Path file : copyFiles.values()) {
      String fixedFile = truncateFileName(truncateLength, file);
      if (fixedFile.startsWith(OM_CHECKPOINT_DIR)) {
        // checkpoint files go to root of tarball
        fixedFile = Paths.get(fixedFile).getFileName().toString();
      }
      includeFile(file.toFile(), fixedFile,
          archiveOutputStream);
    }

    //  Create list of hard links
    if (!hardLinkFiles.isEmpty()) {
      Path hardLinkFile = createHardLinkList(truncateLength, hardLinkFiles);
      includeFile(hardLinkFile.toFile(), OmSnapshotManager.OM_HARDLINK_FILE,
          archiveOutputStream);
    }
  }

  private OzoneConfiguration getConf() {
    return ((OzoneManager) getServletContext()
        .getAttribute(OzoneConsts.OM_CONTEXT_ATTRIBUTE))
        .getConfiguration();
  }
}
