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
import org.apache.hadoop.ozone.shell.OzoneAddress;
import picocli.CommandLine;

import java.io.IOException;

/**
 * ozone snapshot create.
 */
@CommandLine.Command(name = "create",
    description = "create snapshot")
public class SnapshotCreateHandler extends SnapshotHandler {

  @CommandLine.Parameters(description = "mask", arity = "1..1")
  private String mask;

  @Override
  protected void execute(OzoneClient client, OzoneAddress address)
      throws IOException {

    client.getObjectStore().createSnapshot(mask);
    if (isVerbose()) {
      err().format("created snapshot '%s'.%n", mask);
    }
  }
}
