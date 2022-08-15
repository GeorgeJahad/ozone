package org.apache.hadoop.ozone.om;

import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.AuditLoggerType;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.security.acl.OzoneObjInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.hadoop.ozone.om.OmSnapshotManager.denormalizeKeyName;
import static org.apache.hadoop.ozone.om.OmSnapshotManager.normalizeKeyName;

public class OmSnapshot implements IOmMReader {

  public static final Logger LOG =
      LoggerFactory.getLogger(OmSnapshot.class);

  private static final AuditLogger AUDIT = new AuditLogger(
      AuditLoggerType.OMLOGGER);

  private final OmMReader omMReader;
  private final String snapshotName;
  
  public OmSnapshot(KeyManager keyManager,
                          PrefixManager prefixManager,
                          OMMetadataManager omMetadataManager,
                    OzoneManager ozoneManager,
                    String snapshotName) {
    omMReader = new OmMReader(keyManager, prefixManager, omMetadataManager, ozoneManager, LOG, AUDIT, OmSnapshotMetrics.getInstance());
    this.snapshotName = snapshotName;
  }


  @Override
  public OmKeyInfo lookupKey(OmKeyArgs args) throws IOException {
    return denormalizeOmKeyInfo(omMReader.lookupKey(normalizeOmKeyArgs(args)));
  }

  private static OmKeyInfo  normalizeOmKeyInfo(OmKeyInfo keyInfo) {
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

  private static OmKeyArgs normalizeOmKeyArgs(OmKeyArgs args) {
    if (args == null) {
      return null;
    }
    return args.toBuilder().setKeyName(normalizeKeyName(args.getKeyName())).build();
  }

  @Override
  public List<OzoneFileStatus> listStatus(OmKeyArgs args, boolean recursive,
      String startKey, long numEntries, boolean allowPartialPrefixes)
      throws IOException {
    List<OzoneFileStatus> l = omMReader.listStatus(normalizeOmKeyArgs(args), recursive, normalizeKeyName(startKey), numEntries, allowPartialPrefixes);
    return l.stream().map(this::denormalizeOzoneFileStatus).collect(Collectors.toList());
  }

  @Override
  public OzoneFileStatus getFileStatus(OmKeyArgs args) throws IOException {
    return denormalizeOzoneFileStatus(omMReader.getFileStatus(normalizeOmKeyArgs(args)));
  }

  private  OzoneFileStatus denormalizeOzoneFileStatus(OzoneFileStatus fileStatus) {
    return new OzoneFileStatus(denormalizeOmKeyInfo(
        fileStatus.getKeyInfo()),fileStatus.getBlockSize(), fileStatus.isDirectory());
  }

  @Override
  public OmKeyInfo lookupFile(OmKeyArgs args) throws IOException {
    return denormalizeOmKeyInfo(omMReader.lookupFile(normalizeOmKeyArgs(args)));
  }

  @Override
  public List<OmKeyInfo> listKeys(String volumeName, String bucketName,
      String startKey, String keyPrefix, int maxKeys) throws IOException {
    List<OmKeyInfo> l = omMReader.listKeys(volumeName, bucketName, normalizeKeyName(startKey), normalizeKeyName(keyPrefix), maxKeys);
    return l.stream().map(this::denormalizeOmKeyInfo).collect(Collectors.toList());
  }

  @Override
  public List<OzoneAcl> getAcl(OzoneObj obj) throws IOException {
    return omMReader.getAcl(normalizeOzoneObj(obj));
  }

  private OzoneObj normalizeOzoneObj(OzoneObj o) {
    return OzoneObjInfo.Builder.getBuilder(o.getResourceType(),
        o.getStoreType(), o.getVolumeName(), o.getBucketName(),
        normalizeKeyName(o.getKeyName()))
        // OzonePrefixPath field appears to only used by fso delete/rename requests
        //  which are not applicable to snapshots
        .setOzonePrefixPath(o.getOzonePrefixPathViewer()).build();

  }


}
