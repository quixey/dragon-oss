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

import org.apache.hadoop.conf.Configuration;

import javax.annotation.Nonnull;
import java.io.IOException;

import static com.quixey.hadoop.fs.oss.TestProperties.TEST_DIR_PROPERTY;
import static com.quixey.hadoop.fs.oss.TestProperties.TEST_URI_PROPERTY;

/**
 * Tests {@link com.quixey.hadoop.fs.oss.OSSFileSystem} using a stub implementation of the FileSystemStore.
 *
 * @author Jim Lim - jim@quixey.com
 */
public class InMemoryFileSystemContractTest extends OSSFileSystemContractBaseTest {

  @Override
  @Nonnull
  FileSystemStore getFileSystemStore() throws IOException {
    return new InMemoryFileSystemStore();
  }

  @Override
  protected void setUp() throws Exception {
    conf = new Configuration();
    conf.set(TEST_URI_PROPERTY, "mem://xyz");
    conf.set(TEST_DIR_PROPERTY, "/tmp/dragon-tmp");
    conf.set(OSSFileSystemConfigKeys.OSS_BUFFER_DIR_PROPERTY, "/tmp/dragon-test");
    super.setUp();
  }
}
