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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.ozone.shell.snapshot;

import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.shell.Handler;
import org.apache.hadoop.ozone.shell.OzoneAddress;
import picocli.CommandLine;

import java.io.IOException;

/**
 * ozone snapshot create.
 */
@CommandLine.Command(name = "create",
    description = "create snapshot")
public class CreateSnapshotHandler extends Handler {

  @CommandLine.Parameters(description =
      "snapshotPath, (currently only bucket allowed)",
      index = "0", arity = "1..1")
  private String snapshotPath;

  @CommandLine.Parameters(description = "optional snapshot name",
      index = "1", arity = "0..1")
  private String name;

  @Override
  protected void execute(OzoneClient client, OzoneAddress address)
      throws IOException {

    String newName = client.getObjectStore().createSnapshot(name, snapshotPath);
    if (isVerbose()) {
      out().format("created snapshot '%s %s'.%n", snapshotPath, newName);
    }
  }
}
