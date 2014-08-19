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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import org.apache.hadoop.conf.Configuration;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

/**
 * An abstraction for a key-based file store. Adapted from the S3NativeFileSystem.
 *
 * @author Jim Lim - jim@quixey.com
 * @see org.apache.hadoop.fs.s3native.NativeFileSystemStore
 */
@VisibleForTesting
interface FileSystemStore {

  public static final String PATH_DELIMITER = "/";

  /**
   * Initializes the file store. Invoked after the store is constructed.
   *
   * @param uri  store URI
   * @param conf hadoop configuration
   * @throws IOException
   */
  void initialize(URI uri, Configuration conf) throws IOException;

  /**
   * Stores {@code file} at {@code key}, with given {@code md5Hash}.
   *
   * @param key     OSS key
   * @param file    contents
   * @param md5Hash checksum
   * @throws IOException
   */
  void storeFile(String key, File file, Optional<byte[]> md5Hash) throws IOException;

  /**
   * Stores an empty file at {@code key}.
   *
   * @param key location
   * @throws IOException
   */
  void storeEmptyFile(String key) throws IOException;

  /**
   * Retrieves metadata of file identified by given {@code key}.
   *
   * @param key OSS key
   * @return metadata, or {@code null} if {@code key} does not exist.
   * @throws IOException
   */
  @Nullable
  FileMetadata retrieveMetadata(String key) throws IOException;

  /**
   * Retrieves contents of file at given {@code key}.
   *
   * @param key OSS key
   * @return input stream
   * @throws IOException
   */
  @Nonnull
  InputStream retrieve(String key) throws IOException;

  @Nonnull
  InputStream retrieve(String key, long byteRangeStart) throws IOException;

  /**
   * Lists all fields with the given prefix.
   *
   * @param prefix           search prefix
   * @param maxListingLength maximum listing length
   * @return input stream
   * @throws IOException
   */
  @Nonnull
  PartialListing list(String prefix, int maxListingLength) throws IOException;

  /**
   * Retrieves the next chunk of the file listing.
   *
   * @param prefix           search prefix
   * @param maxListingLength maximum listing length
   * @return input stream
   * @throws IOException
   */
  @Nonnull
  PartialListing list(String prefix, int maxListingLength, @Nullable String marker, boolean recursive) throws IOException;

  /**
   * Deletes file/directory at the given {@code key}.
   *
   * @param key location
   * @throws IOException
   */
  void delete(String key) throws IOException;

  /**
   * Copies a file from {code srcKey} to {@code dstKey}.
   *
   * @param srcKey source location
   * @param dstKey destination location
   * @throws IOException
   */
  void copy(String srcKey, String dstKey) throws IOException;

  /**
   * Delete all keys with the given prefix. Used for testing.
   *
   * @throws IOException
   */
  @SuppressWarnings("unused")
  void purge(String prefix) throws IOException;
}
