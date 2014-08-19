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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Holds information on a directory listing for a
 * {@link FileSystemStore}.
 * <p>
 * This includes the {@link FileMetadata} files and directories (their names) contained in a directory.
 * </p>
 * <p>
 * This listing may be returned in chunks, so a {@code marker} is provided so that the next chunk may be requested.
 * </p>
 *
 * @author Jim Lim - jim@quixey.com
 */
@VisibleForTesting
class PartialListing {

  private final String         marker;
  private final FileMetadata[] files;
  private final String[]       commonPrefixes;

  public PartialListing(@Nullable String marker,
                        FileMetadata[] files,
                        String[] commonPrefixes) {
    this.marker = marker;
    this.files = checkNotNull(files);
    this.commonPrefixes = checkNotNull(commonPrefixes);
  }

  @Nonnull
  public FileMetadata[] getFiles() {
    return files;
  }

  @Nonnull
  public String[] getCommonPrefixes() {
    return commonPrefixes;
  }

  @Nullable
  public String getMarker() {
    return marker;
  }
}
