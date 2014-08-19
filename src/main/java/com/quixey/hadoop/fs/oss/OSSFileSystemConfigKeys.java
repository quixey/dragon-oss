/* Copyright 2014, Quixey Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.quixey.hadoop.fs.oss;

/**
 * All configuration properties for OSSFileSystem.
 *
 * @author Jim Lim - jim@quixey.com
 */
public interface OSSFileSystemConfigKeys {

  /**
   * OSS Credentials
   */
  String ACCESS_KEY_PROPERTY = "fs.oss.accessKeyId";

  /**
   * OSS Credentials
   */
  String SECRET_ACCESS_KEY_PROPERTY = "fs.oss.secretAccessKey";

  /**
   * Default block size, in bytes. Defaults to 64MB.
   */
  String OSS_BLOCK_SIZE_PROPERTY = "fs.oss.block.size";

  /**
   * Maximum number of retries. Defaults to 4.
   */
  String OSS_MAX_RETRIES_PROPERTY = "fs.oss.max.retries";

  /**
   * Number of seconds to sleep between retries. Defaults to 10.
   */
  String OSS_SLEEP_TIME_SECONDS_PROPERTY = "fs.oss.sleep.time.seconds";

  /**
   * Path to local directory where temp files are kept before being uploaded to OSS. Required for writes.
   */
  String OSS_BUFFER_DIR_PROPERTY = "fs.oss.buffer.dir";

  /**
   * Maximum number of items to return per listing.
   */
  String OSS_MAX_LISTING_LENGTH_PROPERTY = "fs.oss.max.listing.length";

  /**
   * Enable/disable multipart uploading. Defaults to true.
   */
  String OSS_MULTIPART_UPLOADS_ENABLED = "fs.oss.multipart.uploads.enabled";

  /**
   * Size of each part, in bytes. Defaults to 64MB.
   */
  String OSS_MULTIPART_UPLOADS_BLOCK_SIZE_PROPERTY = "fs.oss.multipart.uploads.block.size";

  /**
   * Maximum number of threads to use. Defaults to Integer.MAX_VALUE.
   */
  String OSS_MULTIPART_UPLOADS_MAX_THREADS_PROPERTY = "fs.oss.multipart.uploads.max.threads";
}