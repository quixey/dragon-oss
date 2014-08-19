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

import com.google.common.base.Objects;

import javax.annotation.Nonnull;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Holds basic metadata for a file stored in a {@link FileSystemStore}.
 *
 * @author Jim Lim - jim@quixey.com
 */
class FileMetadata {
  private final String key;
  private final long   length;
  private final long   lastModified;

  public FileMetadata(String key, long length, long lastModified) {
    this.key = checkNotNull(key);
    this.length = length;
    this.lastModified = lastModified;
  }

  @Nonnull
  public String getKey() {
    return key;
  }

  public long getLength() {
    return length;
  }

  public long getLastModified() {
    return lastModified;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
                  .add("key", key)
                  .add("length", length)
                  .add("last modified", lastModified)
                  .toString();
  }
}