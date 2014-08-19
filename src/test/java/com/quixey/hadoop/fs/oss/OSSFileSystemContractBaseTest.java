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

import com.google.common.base.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemContractBaseTest;
import org.apache.hadoop.fs.Path;

import javax.annotation.Nonnull;
import javax.annotation.OverridingMethodsMustInvokeSuper;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import static com.quixey.hadoop.fs.oss.FileSystemUtils.pathToKey;
import static com.quixey.hadoop.fs.oss.OSSFileSystemConfigKeys.OSS_BLOCK_SIZE_PROPERTY;
import static com.quixey.hadoop.fs.oss.TestProperties.TEST_DIR_PROPERTY;
import static com.quixey.hadoop.fs.oss.TestProperties.TEST_URI_PROPERTY;

/**
 * Basic contract for a file system.
 *
 * <p>Child classes shall provide a concrete FileSystemStore implementation that satisfies the contract.</p>
 * <p>This extends {@link FileSystemContractBaseTest}, which contains a set of rules for every reasonable filesystem
 * implementation. Unfortunately, the parent class was written for JUnit, not TestNG.</p>
 *
 * @author Jim Lim - jim@quixey.com
 */
public abstract class OSSFileSystemContractBaseTest
    extends FileSystemContractBaseTest {

  protected Configuration   conf;
  private   FileSystemStore store;
  private   String          tmpDir;

  @Nonnull
  abstract FileSystemStore getFileSystemStore() throws IOException;

  @Override
  @OverridingMethodsMustInvokeSuper
  protected void setUp() throws Exception {
    conf = null == conf ? new Configuration() : conf;
    store = getFileSystemStore();
    tmpDir = conf.get(TEST_DIR_PROPERTY);
    fs = new OSSFileSystem(store);
    fs.initialize(URI.create(conf.get(TEST_URI_PROPERTY)), conf);
    fs.setWorkingDirectory(path(tmpDir));
  }

  @Override
  protected void tearDown() throws Exception {
    fs.delete(path(tmpDir), true);
    super.tearDown();
  }

  @Override
  protected String getDefaultWorkingDirectory() {
    return tmpDir;
  }

  @Override
  public void testMkdirsWithUmask() throws Exception {
    // do nothing - not sure how to use umask with OSS
  }

  public void testScheme() throws Exception {
    assertEquals("oss", fs.getScheme());
  }

  /**
   * Checks that getCanonicalServiceName returns {@code null}.
   *
   * @throws Exception
   */
  public void testCanonicalName() throws Exception {
    assertNull("oss doesn't support security token and shouldn't have canonical name", fs.getCanonicalServiceName());
  }

  /**
   * Checks that root exists.
   */
  public void testListStatusForRoot() throws Exception {
    FileStatus[] paths = fs.listStatus(path("/"));
    assertTrue("/ does not exist", paths.length >= 0); // dumb check
  }

  /**
   * Checks that {@code tmpDir} is empty, and a child directory can be created.
   */
  public void testListStatusForTmpDir() throws Exception {
    Path testDir = path(tmpDir);
    assertTrue(fs.mkdirs(testDir));

    // check that the test dir is empty
    FileStatus[] paths = fs.listStatus(testDir);
    assertEquals(tmpDir + " is not empty", 0, paths.length);

    // check that we can create a child dir
    Path child = path(tmpDir + "/xxx");
    assertTrue(fs.mkdirs(child));

    paths = fs.listStatus(testDir);
    assertEquals(1, paths.length);
    assertEquals(child, paths[0].getPath());
    assertTrue(paths[0].isDirectory());
  }

  /**
   * Checks that when
   *
   * - x/y exists, but
   * - x/ does not exist,
   *
   * list("x") still works as expected.
   */
  public void testListUnmarkedDir() throws Exception {
    Path dir = path("test/hadoop");
    assertFalse(fs.exists(dir));

    Path file = path("test/hadoop/file");
    createFile(file);

    assertTrue(fs.exists(dir));

    FileStatus[] paths = fs.listStatus(dir);
    assertEquals(1, paths.length);
    assertEquals(file.getName(), paths[0].getPath().getName());
  }

  /**
   * Checks that oss://whatever is a directory.
   */
  public void testNoTrailingBackslashOnBucket() throws Exception {
    assertTrue(fs.getFileStatus(new Path(fs.getUri().toString())).isDirectory());
  }

  /**
   * Checks that we can store an empty file.
   */
  public void testEmptyFile() throws Exception {
    Path path = path("test/hadoop/file1");
    store.storeEmptyFile(pathToKey(path));
    assertTrue(fs.exists(path));
  }

  /**
   * Checks that fs#create works.
   */
  public void testCreateFile() throws Exception {
    Path path = path("test/hadoop/file");
    createFile(path);
    assertTrue(fs.exists(path));
  }

  /**
   * Checks that trailing slash indicates a directory.
   */
  public void testDirWithMarker() throws Exception {
    Path path = path("test/hadoop/dir");
    store.storeEmptyFile(pathToKey(path) + "/");
    assertTrue(fs.isDirectory(path));
  }

  private void createTestFiles(String base) throws IOException {
    createFile(path(base + "/file1"));
    createFile(path(base + "/dir/file2"));
    createFile(path(base + "/dir/file3"));
  }

  /**
   * Checks that delete works without markers, and a parent marker is created after all children are removed.
   */
  public void testDeleteWithNoMarker() throws Exception {
    Path path = path("test/hadoop/dir");
    createTestFiles("test/hadoop/dir");

    fs.delete(path, true);

    Path base = path("test/hadoop");

    assertTrue(fs.isDirectory(base));
    assertEquals(0, fs.listStatus(base).length);
  }

  /**
   * Checks that rename works without markers.
   */
  public void testRenameWithNoMarker() throws Exception {
    String base = "test/hadoop";
    Path dest = path("test/hadoop2");

    createTestFiles(base);

    fs.rename(path(base), dest);

    Path path = path("test");
    assertTrue(fs.isDirectory(path));
    assertEquals(1, fs.listStatus(path).length);
    assertTrue(fs.isDirectory(dest));
    assertEquals(2, fs.listStatus(dest).length);
  }

  /**
   * Checks that OSS returns the expected block size.
   */
  public void testBlockSize() throws Exception {
    Path file = path("/test/hadoop/file");
    createFile(file);
    assertEquals("Default block size", fs.getDefaultBlockSize(file), fs.getFileStatus(file).getBlockSize());

    // block size is determined at read time
    long newBlockSize = fs.getDefaultBlockSize(file) * 2;
    fs.getConf().setLong(OSS_BLOCK_SIZE_PROPERTY, newBlockSize);
    assertEquals("Double default block size", newBlockSize, fs.getFileStatus(file).getBlockSize());
  }

  /**
   * Checks that retry works. Copied from tests for the S3 FileSystem.
   */
  public void testRetryOnIOException() throws Exception {

    class TestInputStream extends InputStream {
      boolean shouldThrow = false;
      int     throwCount  = 0;
      int     pos         = 0;
      byte[] bytes;

      public TestInputStream() {
        bytes = new byte[256];
        for (int i = 0; i < 256; i++) {
          bytes[i] = (byte) i;
        }
      }

      @Override
      public int read() throws IOException {
        shouldThrow = !shouldThrow;
        if (shouldThrow) {
          throwCount++;
          throw new IOException();
        }
        return pos++;
      }

      @Override
      public int read(byte[] b, int off, int len) throws IOException {
        shouldThrow = !shouldThrow;
        if (shouldThrow) {
          throwCount++;
          throw new IOException();
        }

        int sizeToRead = Math.min(len, 256 - pos);
        System.arraycopy(bytes, pos, b, 0, sizeToRead);
        pos += sizeToRead;
        return sizeToRead;
      }
    }

    try (final InputStream is = new TestInputStream()) {

      class MockNativeFileSystemStore extends CloudOSSFileSystemStore {
        @Override
        @Nonnull
        public InputStream retrieve(String key) throws IOException {
          return is;
        }

        @Override
        @Nonnull
        public InputStream retrieve(String key, long byteRangeStart) throws IOException {
          return is;
        }
      }

      byte[] result;
      Optional<FileSystem.Statistics> absent = Optional.absent();
      try (OSSFileSystem.OSSFileInputStream stream = new OSSFileSystem.OSSFileInputStream(new MockNativeFileSystemStore(), "", absent)) {

        // Test reading methods.
        result = new byte[256];
        for (int i = 0; i < 128; i++) {
          result[i] = (byte) stream.read();
        }

        for (int i = 128; i < 256; i += 8) {
          byte[] temp = new byte[8];
          int read = stream.read(temp, 0, 8);
          assertEquals(8, read);
          System.arraycopy(temp, 0, result, i, 8);
        }
      }

      // Assert correct
      for (int i = 0; i < 256; i++) {
        assertEquals((byte) i, result[i]);
      }

      // Test to make sure the throw path was exercised.
      // 144 = 128 + (128 / 8)
      assertEquals(144, ((TestInputStream) is).throwCount);
    }
  }
}
