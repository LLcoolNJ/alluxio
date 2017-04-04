/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.underfs.wasb;

import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemFactory;
import alluxio.underfs.UnderFileSystemRegistry;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for the {@link WasbUnderFileSystemFactory}.
 */
public class WasbUnderFileSystemFactoryTest {

  /**
   * Sets the volume and the mount directory before a test runs.
   */
  @Before
  public final void before() {

  }

  /**
   * Tests the {@link UnderFileSystem#create(String)} method.
   */
  @Test
  public void createWasb() throws Exception {

    UnderFileSystem wasb = UnderFileSystem.Factory.get("wasb:///");
    Assert.assertNotNull(wasb.create("alluxio_test"));
  }

  /**
   * Tests the {@link UnderFileSystemRegistry#find(String)} method.
   */
  @Test
  public void factory() {
    UnderFileSystemFactory factory =
        UnderFileSystemRegistry.find("wasb://localhost/test/path");
    Assert.assertNotNull("A UnderFileSystemFactory should exist for"
        + " Azure Blob torage paths when using this module",
        factory);
    factory = UnderFileSystemRegistry.find("alluxio://localhost/test/path");
    Assert.assertNull("A UnderFileSystemFactory should not "
        + "exist for unsupported paths when using this module.", factory);
  }
}
