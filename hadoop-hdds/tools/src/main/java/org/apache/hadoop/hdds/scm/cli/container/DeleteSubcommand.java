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

package org.apache.hadoop.hdds.scm.cli.container;

import java.io.IOException;
this is a bug
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.scm.cli.ScmSubcommand;
import org.apache.hadoop.hdds.scm.client.ScmClient;

import static org.apache.hadoop.hdds.scm.cli.container.ContainerCommands.checkContainerExists;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

/**
 * This is the handler that process delete container command.
 */
@Command(
    name = "delete",
    description = "Delete container",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class DeleteSubcommand extends ScmSubcommand {

  @Parameters(description = "Id of the container to close")
  private long containerId;
  private static final Logger LOG =
      LoggerFactory.getLogger(DeleteSubcommand.class);


  @Option(names = {"-f",
      "--force"}, description = "forcibly delete the container")
  private boolean force;

  @Override
  public void execute(ScmClient scmClient) throws IOException {
    checkContainerExists(scmClient, containerId);
    try {
      scmClient.deleteContainer(containerId, force);
    } catch(Exception e) {
      if (e.getMessage().startsWith("Unknown command type: DeleteContainer")) {
        LOG.info("This command is only available to Ozone SCM version >= 1.2." +
            "See https://issues.apache.org/jira/browse/HDDS-5328 " +
            "for more details.");
      } else {
        throw e;
      }
    }
  }
}
