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
import com.google.common.base.Optional;
import org.apache.hadoop.conf.Configuration;

import java.net.URI;

import static com.google.common.base.Optional.absent;
import static com.google.common.base.Optional.fromNullable;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.quixey.hadoop.fs.oss.OSSFileSystemConfigKeys.ACCESS_KEY_PROPERTY;
import static com.quixey.hadoop.fs.oss.OSSFileSystemConfigKeys.SECRET_ACCESS_KEY_PROPERTY;

/**
 * Extracts OSS credentials from the filesystem URI or configuration.
 * <ul>
 * <li>oss://[id]:[secret]@bucket/, or</li>
 * <li>{@value com.quixey.hadoop.fs.oss.OSSFileSystemConfigKeys#ACCESS_KEY_PROPERTY},
 * {@value com.quixey.hadoop.fs.oss.OSSFileSystemConfigKeys#SECRET_ACCESS_KEY_PROPERTY}</li>
 * </ul>
 *
 * @author Jim Lim - jim@quixey.com
 */
public class OSSCredentials {

  private Optional<String> accessKeyId     = absent();
  private Optional<String> secretAccessKey = absent();

  public void initialize(URI uri, Configuration conf) {
    uri = checkNotNull(uri);
    conf = checkNotNull(conf);

    String userInfo = uri.getUserInfo();
    if (null != userInfo) parseUserInfo(userInfo);

    parseConfig(conf);
  }

  public String getAccessKeyId() {
    return accessKeyId.get();
  }

  public String getSecretAccessKey() {
    return secretAccessKey.get();
  }

  public String toString() {
    return Objects.toStringHelper(this)
                  .add("access key id", accessKeyId)
                  .add("secret access key", secretAccessKey)
                  .toString();
  }

  /**
   * Attempts to extract access credentials from user info.
   * <ul>
   * <li>id:secret</li>
   * <li>secret</li>
   * </ul>
   */
  private void parseUserInfo(String userInfo) {
    int index = userInfo.indexOf(':');
    if (-1 != index) {
      accessKeyId = Optional.of(userInfo.substring(0, index));
      secretAccessKey = Optional.of(userInfo.substring(index + 1));
    } else {
      secretAccessKey = Optional.of(userInfo);
    }
  }

  /**
   * Attempts to extract credentials from configuration.
   */
  private void parseConfig(Configuration conf) {

    accessKeyId = accessKeyId.or(fromNullable(conf.get(ACCESS_KEY_PROPERTY)));
    secretAccessKey = secretAccessKey.or(fromNullable(conf.get(SECRET_ACCESS_KEY_PROPERTY)));

    checkArgument(accessKeyId.isPresent() && secretAccessKey.isPresent(),
        "OSS Access Key ID and Secret Access Key must be specified in the URL, or by setting the " +
        ACCESS_KEY_PROPERTY + " and " +
        SECRET_ACCESS_KEY_PROPERTY +
        " properties.");
  }
}
