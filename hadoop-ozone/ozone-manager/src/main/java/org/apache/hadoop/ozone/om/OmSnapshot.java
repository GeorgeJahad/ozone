package org.apache.hadoop.ozone.om;

import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.AuditLoggerType;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.security.acl.OzoneObjInfo;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class OmSnapshot implements IOmMReader {

  public static final Logger LOG =
      LoggerFactory.getLogger(OmSnapshot.class);

  private static final AuditLogger AUDIT = new AuditLogger(
      AuditLoggerType.OMLOGGER);

  private final OmMReader omMReader;
  private final String volumeName;
  private final String bucketName;
  private final String snapshotName;
  private final KeyManager keyManager;
  
  public OmSnapshot(KeyManager keyManager,
                    PrefixManager prefixManager,
                    OMMetadataManager omMetadataManager,
                    OzoneManager ozoneManager,
                    String volumeName,
                    String bucketName,
                    String snapshotName) {
    omMReader = new OmMReader(keyManager, prefixManager,
        omMetadataManager, ozoneManager, LOG, AUDIT,
        OmSnapshotMetrics.getInstance());
    this.snapshotName = snapshotName;
    this.bucketName = bucketName;
    this.volumeName = volumeName;
  }


  @Override
  public OmKeyInfo lookupKey(OmKeyArgs args) throws IOException {
    return denormalizeOmKeyInfo(omMReader.lookupKey(
        normalizeOmKeyArgs(args)));
  }

  @Override
  public List<OzoneFileStatus> listStatus(OmKeyArgs args, boolean recursive,
      String startKey, long numEntries, boolean allowPartialPrefixes)
      throws IOException {
    List<OzoneFileStatus> l = omMReader.listStatus(normalizeOmKeyArgs(args),
        recursive, normalizeKeyName(startKey), numEntries, allowPartialPrefixes);
    return l.stream().map(this::denormalizeOzoneFileStatus).collect(Collectors.toList());
  }

  @Override
  public OzoneFileStatus getFileStatus(OmKeyArgs args) throws IOException {
    return denormalizeOzoneFileStatus(
        omMReader.getFileStatus(normalizeOmKeyArgs(args)));
  }

  @Override
  public OmKeyInfo lookupFile(OmKeyArgs args) throws IOException {
    return denormalizeOmKeyInfo(omMReader
        .lookupFile(normalizeOmKeyArgs(args)));
  }

  @Override
  public List<OmKeyInfo> listKeys(String volumeName, String bucketName,
      String startKey, String keyPrefix, int maxKeys) throws IOException {
    List<OmKeyInfo> l = omMReader.listKeys(volumeName, bucketName,
        normalizeKeyName(startKey), normalizeKeyName(keyPrefix), maxKeys);
    return l.stream().map(this::denormalizeOmKeyInfo)
        .collect(Collectors.toList());
  }

  @Override
  public List<OzoneAcl> getAcl(OzoneObj obj) throws IOException {
    return omMReader.getAcl(normalizeOzoneObj(obj));
  }

  private OzoneObj normalizeOzoneObj(OzoneObj o) {
    if (o == null) {
      return null;
    }

    return OzoneObjInfo.Builder.getBuilder(o.getResourceType(),
        o.getStoreType(), o.getVolumeName(), o.getBucketName(),
        normalizeKeyName(o.getKeyName()))
        // OzonePrefixPath field appears to only used by fso delete/rename requests
        //  which are not applicable to snapshots
        .setOzonePrefixPath(o.getOzonePrefixPathViewer()).build();

  }


  // Remove snapshot indicator from keyname
  private String normalizeKeyName(String keyname) {
    if (keyname == null) {
      return null;
    }
    String[] keyParts = keyname.split("/");
    if ((keyParts.length > 1) && (keyParts[0].compareTo(".snapshot") == 0)) {
      if (keyParts.length == 2) {
        return "";
      }
      String normalizedKeyName = String.join("/", Arrays.copyOfRange(keyParts, 2, keyParts.length));
      if (keyname.endsWith("/")) {
        normalizedKeyName = normalizedKeyName + "/";
      }
      return normalizedKeyName;
    }
    return keyname;
  }

  // Restore snapshot indicator to keyanme
  private String denormalizeKeyName(String keyname, String snapshotName) {
    if (keyname == null) {
      return null;
    }
    return ".snapshot/" + snapshotName + "/" + keyname;
  }
  private OmKeyInfo  normalizeOmKeyInfo(OmKeyInfo keyInfo) {
    if (keyInfo == null) {
      return null;
    }
    OmKeyInfo normalized = keyInfo.copyObject();
    normalized.setKeyName(normalizeKeyName(keyInfo.getKeyName()));
    return normalized;
  }

  private  OmKeyInfo  denormalizeOmKeyInfo(OmKeyInfo keyInfo) {
    if (keyInfo == null) {
      return null;
    }
    OmKeyInfo denormalized = keyInfo.copyObject();
    denormalized.setKeyName(denormalizeKeyName(keyInfo.getKeyName(), snapshotName));
    return denormalized;
  }

  private OmKeyArgs normalizeOmKeyArgs(OmKeyArgs args) {
    if (args == null) {
      return null;
    }
    return args.toBuilder().setKeyName(normalizeKeyName(args.getKeyName())).build();
  }

  private  OzoneFileStatus denormalizeOzoneFileStatus(OzoneFileStatus fileStatus) {
    if (fileStatus == null) {
      return null;
    }
    OmKeyInfo omKeyInfo;
    if (fileStatus.getKeyInfo() == null) {
    omKeyInfo = new OmKeyInfo.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName("")
        .setOmKeyLocationInfos(Collections.singletonList(
            new OmKeyLocationInfoGroup(0, new ArrayList<>())))
        .setCreationTime(Time.now())
        .setModificationTime(Time.now())
        .setDataSize(0)
        .setReplicationConfig(RatisReplicationConfig
            .getInstance(HddsProtos.ReplicationFactor.ONE))
        .build();
    } else {
      omKeyInfo = fileStatus.getKeyInfo();
    }
    return new OzoneFileStatus(denormalizeOmKeyInfo(
        omKeyInfo),fileStatus.getBlockSize(), fileStatus.isDirectory());
  }

}
