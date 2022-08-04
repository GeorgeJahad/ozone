package org.apache.hadoop.ozone.om;

import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.AuditLoggerType;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class OmSnapshot implements IOmMReader {

  public static final Logger LOG =
      LoggerFactory.getLogger(OmSnapshot.class);

  private static final AuditLogger AUDIT = new AuditLogger(
      AuditLoggerType.OMLOGGER);

  private final OmMReader omMReader;
  
  public OmSnapshot(KeyManager keyManager,
                          PrefixManager prefixManager,
                          OMMetadataManager omMetadataManager,
                    OzoneManager ozoneManager) {
    omMReader = new OmMReader(keyManager, prefixManager, omMetadataManager, ozoneManager, LOG, AUDIT, OmSnapshotMetrics.getInstance());
  }


  @Override
  public OmKeyInfo lookupKey(OmKeyArgs args) throws IOException {
    return omMReader.lookupKey(args);
  }

  @Override
  public List<OzoneFileStatus> listStatus(OmKeyArgs args, boolean recursive,
      String startKey, long numEntries, boolean allowPartialPrefixes)
      throws IOException {
    return omMReader.listStatus(args, recursive, startKey, numEntries, allowPartialPrefixes);
  }

  @Override
  public OzoneFileStatus getFileStatus(OmKeyArgs args) throws IOException {
    return omMReader.getFileStatus(args);
  }

  @Override
  public OmKeyInfo lookupFile(OmKeyArgs args) throws IOException {
    return omMReader.lookupFile(args);
  }

  @Override
  public List<OmKeyInfo> listKeys(String volumeName, String bucketName,
      String startKey, String keyPrefix, int maxKeys) throws IOException {
    return omMReader.listKeys(volumeName, bucketName, startKey, keyPrefix, maxKeys);
  }

  @Override
  public List<OzoneAcl> getAcl(OzoneObj obj) throws IOException {
    return omMReader.getAcl(obj);
  }

  
}
