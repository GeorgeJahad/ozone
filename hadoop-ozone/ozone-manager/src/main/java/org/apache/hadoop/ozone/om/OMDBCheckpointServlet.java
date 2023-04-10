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

import org.apache.commons.compress.archivers.ArchiveOutputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.recon.ReconConfig;
import org.apache.hadoop.hdds.server.ServerUtils;
import org.apache.hadoop.hdds.utils.DBCheckpointServlet;
import org.apache.hadoop.hdds.utils.db.DBCheckpoint;
import org.apache.hadoop.hdds.utils.db.RDBCheckpointUtils;
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hadoop.hdds.utils.HddsServerUtil.includeFile;
import static org.apache.hadoop.ozone.OzoneConsts.OM_CHECKPOINT_DIR;
import static org.apache.hadoop.ozone.OzoneConsts.OM_SNAPSHOT_CHECKPOINT_DIR;
import static org.apache.hadoop.ozone.OzoneConsts.OM_SNAPSHOT_DIR;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_DB_CHECKPOINT_INCLUDE_SNAPSHOT_DATA;
import static org.apache.hadoop.ozone.OzoneConsts.ROCKSDB_SST_SUFFIX;
import static org.apache.hadoop.ozone.om.snapshot.OmSnapshotUtils.createHardLinkList;
import static org.apache.hadoop.ozone.om.OmSnapshotManager.getSnapshotPath;
import static org.apache.hadoop.ozone.om.snapshot.OmSnapshotUtils.truncateFileName;

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
  public List<String> writeDbDataToStream(DBCheckpoint checkpoint,
                                  HttpServletRequest request,
                                  OutputStream destination,
                                  List<String> toExcludeList)
      throws IOException, InterruptedException {
    List<String> excluded = new ArrayList<>();

    // Files to be copied
    Set<Path> copyFiles = new HashSet<>();
    // Map of link to path.
    Map<Path, Path> hardLinkFiles = new HashMap<>();

    Set<Path> excludeFiles = normalizeExcludeList(toExcludeList, checkpoint);

    getFilesForArchive(checkpoint, copyFiles, hardLinkFiles, excludeFiles,
        includeSnapshotData(request));

    try (TarArchiveOutputStream archiveOutputStream =
            new TarArchiveOutputStream(destination)) {
      archiveOutputStream
          .setLongFileMode(TarArchiveOutputStream.LONGFILE_POSIX);
      writeFilesToArchive(copyFiles, hardLinkFiles, archiveOutputStream);
    }
    return excluded;
  }

  private Set<Path> normalizeExcludeList(List<String> toExcludeList, DBCheckpoint checkpoint) {
    Set<Path> paths = new HashSet<>();
    String metaDirPath = ServerUtils.getOzoneMetaDirPath(getConf()).toString();
    for (String s: toExcludeList) {
      if (!s.startsWith(OM_SNAPSHOT_DIR)) {
        Path fixedPath = Paths.get(checkpoint.getCheckpointLocation().toString(), s);
        // Don't include files that have been compacted away, because they will disappear
        //  when the new sst files are read in.
        if (fixedPath.toFile().exists()) {
          paths.add(fixedPath);
        }
      } else {
        paths.add(Paths.get(metaDirPath, s));
      }
    }
    return paths;
  }

  private void getFilesForArchive(DBCheckpoint checkpoint,
                                  Set<Path> copyFiles,
                                  Map<Path, Path> hardLinkFiles,
                                  Set<Path> excludeFiles,
                                  boolean includeSnapshotData)
      throws IOException {

    // Get the active fs files.
    Path dir = checkpoint.getCheckpointLocation();
    processDir(dir, copyFiles, hardLinkFiles, excludeFiles, new HashSet<>());

    if (!includeSnapshotData) {
      return;
    }

    // Get the snapshot files.
    Set<Path> snapshotPaths = waitForSnapshotDirs(checkpoint);
    Path snapshotDir = Paths.get(OMStorage.getOmDbDir(getConf()).toString(),
        OM_SNAPSHOT_DIR);
    processDir(snapshotDir, copyFiles, hardLinkFiles, excludeFiles, snapshotPaths);
  }

  /**
   * The snapshotInfo table may contain a snapshot that
   * doesn't yet exist on the fs, so wait a few seconds for it.
   * @param checkpoint Checkpoint containing snapshot entries expected.
   * @return Set of expected snapshot dirs.
   */
  private Set<Path> waitForSnapshotDirs(DBCheckpoint checkpoint)
      throws IOException {

    OzoneConfiguration conf = getConf();

    Set<Path> snapshotPaths = new HashSet<>();

    // get snapshotInfo entries
    OmMetadataManagerImpl checkpointMetadataManager =
        OmMetadataManagerImpl.createCheckpointMetadataManager(
            conf, checkpoint);
    try (TableIterator<String, ? extends Table.KeyValue<String, SnapshotInfo>>
        iterator = checkpointMetadataManager
        .getSnapshotInfoTable().iterator()) {

      // For each entry, wait for corresponding directory.
      while (iterator.hasNext()) {
        Table.KeyValue<String, SnapshotInfo> entry = iterator.next();
        Path path = Paths.get(getSnapshotPath(conf, entry.getValue()));
        waitForDirToExist(path);
        snapshotPaths.add(path);
      }
    }
    return snapshotPaths;
  }

  private void waitForDirToExist(Path dir) throws IOException {
    if (!RDBCheckpointUtils.waitForCheckpointDirectoryExist(dir.toFile())) {
      throw new IOException("snapshot dir doesn't exist: " + dir);
    }
  }

  private void processDir(Path dir, Set<Path> copyFiles,
                          Map<Path, Path> hardLinkFiles,
                          Set<Path> excludeFiles,
                          Set<Path> snapshotPaths)
      throws IOException {
    try (Stream<Path> files = Files.list(dir)) {
      for (Path file : files.collect(Collectors.toList())) {
        File f = file.toFile();
        if (f.isDirectory()) {
          // Skip any unexpected snapshot files.
          String parent = f.getParent();
          if (parent != null && parent.endsWith(OM_SNAPSHOT_CHECKPOINT_DIR)
              && !snapshotPaths.contains(file)) {
            LOG.debug("Skipping unneeded file: " + file);
            continue;
          }
          processDir(file, copyFiles, hardLinkFiles, excludeFiles,
              snapshotPaths);
        } else {
          processFile(file, copyFiles, hardLinkFiles, excludeFiles);
        }
      }
    }
  }

  private void processFile(Path file, Set<Path> copyFiles,
                           Map<Path, Path> hardLinkFiles,
                           Set<Path> excludeFiles) {
    if (!excludeFiles.contains(file)) {
      String fileName = file.getFileName().toString();
      if (fileName.endsWith(ROCKSDB_SST_SUFFIX)) {
        // see if there is a link for the sst file
        Path linkPath = findLinkPath(excludeFiles, fileName);
        if (linkPath != null) {
          hardLinkFiles.put(file, linkPath);
        } else {
          linkPath = findLinkPath(copyFiles, fileName);
          if (linkPath != null) {
            hardLinkFiles.put(file, linkPath);
          } else {
            copyFiles.add(file);
          }
        }
      } else {// not sst file
        copyFiles.add(file);
      }
    }
  }

  private Path findLinkPath(Set<Path> files, String fileName) {
    for (Path p: files) {
      if (p.toString().endsWith(fileName)) {
        return p;
      }
    }
    return null;
  }

  // Returns value of http request parameter.
  private boolean includeSnapshotData(HttpServletRequest request) {
    String includeParam =
        request.getParameter(OZONE_DB_CHECKPOINT_INCLUDE_SNAPSHOT_DATA);
    return Boolean.parseBoolean(includeParam);
  }

  private void writeFilesToArchive(Set<Path> copyFiles,
                                   Map<Path, Path> hardLinkFiles,
                                   ArchiveOutputStream archiveOutputStream)
      throws IOException {

    File metaDirPath = ServerUtils.getOzoneMetaDirPath(getConf());
    int truncateLength = metaDirPath.toString().length() + 1;

    // Go through each of the files to be copied and add to archive.
    for (Path file : copyFiles) {
      String fixedFile = truncateFileName(truncateLength, file);
      if (fixedFile.startsWith(OM_CHECKPOINT_DIR)) {
        // checkpoint files go to root of tarball
        Path f = Paths.get(fixedFile).getFileName();
        if (f != null) {
          fixedFile = f.toString();
        }
      }
      includeFile(file.toFile(), fixedFile, archiveOutputStream);
    }

    // Create list of hard links.
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
