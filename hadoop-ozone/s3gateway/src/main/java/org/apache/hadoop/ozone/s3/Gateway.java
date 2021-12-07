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
package org.apache.hadoop.ozone.s3;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.hadoop.hdds.StringUtils;
import org.apache.hadoop.hdds.cli.GenericCli;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.tracing.TracingUtil;
import org.apache.hadoop.ozone.OzoneSecurityUtil;
import org.apache.hadoop.ozone.util.OzoneVersionInfo;

import org.apache.hadoop.ozone.util.ShutdownHookManager;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;

import static org.apache.hadoop.ozone.conf.OzoneServiceConfig.DEFAULT_SHUTDOWN_HOOK_PRIORITY;
import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_KERBEROS_KEYTAB_FILE_KEY;
import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_KERBEROS_PRINCIPAL_KEY;

/**
 * This class is used to start/stop S3 compatible rest server.
 */
@Command(name = "ozone s3g",
    hidden = true, description = "S3 compatible rest server.",
    versionProvider = HddsVersionProvider.class,
    mixinStandardHelpOptions = true)
public class Gateway extends GenericCli {

  private static final Logger LOG = LoggerFactory.getLogger(Gateway.class);

  private S3GatewayHttpServer httpServer;

  public static void main(String[] args) throws Exception {
    waitForOm();
    new Gateway().run(args);
  }

  @Override
  public Void call() throws Exception {
    OzoneConfiguration ozoneConfiguration = createOzoneConfiguration();
    TracingUtil.initTracing("S3gateway", ozoneConfiguration);
    OzoneConfigurationHolder.setConfiguration(ozoneConfiguration);
    UserGroupInformation.setConfiguration(ozoneConfiguration);
    loginS3GUser(ozoneConfiguration);
    httpServer = new S3GatewayHttpServer(ozoneConfiguration, "s3gateway");
    RegistryService.getRegistryService(ozoneConfiguration).start();
    start();

    ShutdownHookManager.get().addShutdownHook(() -> {
      try {
        stop();
      } catch (Exception e) {
        LOG.error("Error during stop S3Gateway", e);
      }
    }, DEFAULT_SHUTDOWN_HOOK_PRIORITY);
    return null;
  }

  public void start() throws IOException {
    String[] originalArgs = getCmd().getParseResult().originalArgs()
        .toArray(new String[0]);
    StringUtils.startupShutdownMessage(OzoneVersionInfo.OZONE_VERSION_INFO,
        Gateway.class, originalArgs, LOG);

    LOG.info("Starting Ozone S3 gateway");
    httpServer.start();
  }

  public void stop() throws Exception {
    LOG.info("Stopping Ozone S3 gateway");
    httpServer.stop();
  }

  private static void loginS3GUser(OzoneConfiguration conf)
      throws IOException, AuthenticationException {
    if (OzoneSecurityUtil.isSecurityEnabled(conf)) {
      if (SecurityUtil.getAuthenticationMethod(conf).equals(
          UserGroupInformation.AuthenticationMethod.KERBEROS)) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Ozone security is enabled. Attempting login for S3G user. "
                  + "Principal: {}, keytab: {}",
              conf.get(OZONE_S3G_KERBEROS_PRINCIPAL_KEY),
              conf.get(OZONE_S3G_KERBEROS_KEYTAB_FILE_KEY));
        }

        SecurityUtil.login(conf, OZONE_S3G_KERBEROS_KEYTAB_FILE_KEY,
            OZONE_S3G_KERBEROS_PRINCIPAL_KEY);
      } else {
        throw new AuthenticationException(SecurityUtil.getAuthenticationMethod(
            conf) + " authentication method not supported. S3G user login "
            + "failed.");
      }
      LOG.info("S3Gateway login successful.");
    }
  }
  // TODO:  switch to k8s init container:
  //  https://kubernetes.io/docs/concepts/workloads/pods/init-containers/
  private static void waitForOm() {
    while (true) {
      try {
        InetAddress.getByName("om-0.om");
        try {
          Thread.sleep(15000);
        } catch (InterruptedException interruptedException) {
        }
        return;
      } catch (UnknownHostException e) {
        LOG.error("om not ready");
        try {
          Thread.sleep(5000);
        } catch (InterruptedException interruptedException) {
        }
      }
    }
  }
}
