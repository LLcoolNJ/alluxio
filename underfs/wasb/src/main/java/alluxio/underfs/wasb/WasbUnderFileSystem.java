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

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.hdfs.HdfsUnderFileSystem;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Azure Blob Storage {@link UnderFileSystem} implementation.
 */

@ThreadSafe
public class WasbUnderFileSystem extends HdfsUnderFileSystem {

  /**
   * Constant for the Azure Blob Storage URI scheme.
   */
  public static final String SCHEME = "wasb://";

  /**
   * Constructs a new Azure Blob Storage {@link UnderFileSystem}.
   *
   * @param uri the {@link AlluxioURI} for this UFS
   * @param conf the configuration for Hadoop or GlusterFS
   */
  public WasbUnderFileSystem(AlluxioURI uri, Object conf) {
    super(uri, conf);
  }

  @Override
  public String getUnderFSType() {
    return "wasb";
  }

  @Override
  protected void prepareConfiguration(String path,
                                      org.apache.hadoop.conf.Configuration hadoopConf) {
    if (path.startsWith(SCHEME)) {
      // Configure for Azure Blob Storage
      hadoopConf.set("fs.AbstractFileSystem.wasb.Impl",
          Configuration.get(PropertyKey.UNDERFS_WASB_IMPL));
      hadoopConf.set(
          "fs.azure.account.key." + Configuration.get(PropertyKey.UNDERFS_WASB_STORAGE_ACCOUNT)
          + ".blob.core.windows.net",
          Configuration.get(PropertyKey.WASB_ACCESS_KEY));
      hadoopConf.set(
          "fs.defaultFS",
          "wasb://" + Configuration.get(PropertyKey.UNDERFS_WASB_CONTAINER) + "@"
          + Configuration.get(PropertyKey.UNDERFS_WASB_STORAGE_ACCOUNT) + ".blob.core.windows.net");
    } else {
      // If not Azure Blob Storage fall back to default HDFS behavior
      // This should only happen if someone creates an instance of this directly rather than via the
      // registry and factory which enforces the wasb prefix being present.
      super.prepareConfiguration(path, hadoopConf);
    }
  }
}
