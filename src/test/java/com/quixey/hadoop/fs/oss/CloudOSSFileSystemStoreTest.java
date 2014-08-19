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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.AccessControlException;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.security.DigestInputStream;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.TreeSet;

import static com.quixey.hadoop.fs.oss.OSSFileSystemConfigKeys.ACCESS_KEY_PROPERTY;
import static com.quixey.hadoop.fs.oss.OSSFileSystemConfigKeys.OSS_MAX_LISTING_LENGTH_PROPERTY;
import static com.quixey.hadoop.fs.oss.OSSFileSystemConfigKeys.OSS_MULTIPART_UPLOADS_BLOCK_SIZE_PROPERTY;
import static com.quixey.hadoop.fs.oss.OSSFileSystemConfigKeys.SECRET_ACCESS_KEY_PROPERTY;
import static com.quixey.hadoop.fs.oss.TestProperties.TEST_DIR_PROPERTY;
import static com.quixey.hadoop.fs.oss.TestProperties.TEST_URI_PROPERTY;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeNotNull;

/**
 * Tests for {@link CloudOSSFileSystemStore}.
 *
 * @author Jim Lim - jim@quixey.com
 */
public class CloudOSSFileSystemStoreTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  private Configuration           conf;
  private CloudOSSFileSystemStore store;
  private OSSFileSystem           fs;
  private String                  tmpDir;

  @BeforeClass
  public static void checkSettings() throws Exception {
    Configuration conf = new Configuration();
    assumeNotNull(conf.get(ACCESS_KEY_PROPERTY));
    assumeNotNull(conf.get(SECRET_ACCESS_KEY_PROPERTY));
    assumeNotNull(conf.get(TEST_URI_PROPERTY));
  }

  @Before
  public void setUp() throws IOException {
    conf = new Configuration();
    store = new CloudOSSFileSystemStore();
    fs = new OSSFileSystem(store);
    tmpDir = conf.get(TEST_DIR_PROPERTY);
    fs.initialize(URI.create(conf.get(TEST_URI_PROPERTY)), conf);
    fs.setWorkingDirectory(new Path(tmpDir));
  }

  @After
  public void tearDown() throws IOException {
    store.purge(tmpDir);
  }

  private void writeRenameReadCompare(Path path, long len) throws NoSuchAlgorithmException, IOException {
    // write files of length `len` to `path`
    MessageDigest digest = MessageDigest.getInstance("MD5");
    try (OutputStream out = new BufferedOutputStream(new DigestOutputStream(fs.create(path), digest))) {
      for (long i = 0; i < len; i++) out.write(74);
    }

    assertTrue(fs.exists(path));

    // rename - might cause a multipart copy
    Path copyPath = path.suffix(".copy");
    fs.rename(path, copyPath);
    assertTrue(fs.exists(copyPath));

    // download the file
    MessageDigest digest2 = MessageDigest.getInstance("MD5");
    long copyLen = 0;
    try (InputStream in = new BufferedInputStream(new DigestInputStream(fs.open(copyPath), digest2))) {
      while (-1 != in.read()) copyLen++;
    }

    // compare lengths, digests
    assertEquals(len, copyLen);
    assertArrayEquals(digest.digest(), digest2.digest());
  }

  @Test
  public void testSmallUpload() throws IOException, NoSuchAlgorithmException {
    Path path = new Path("upload/small.file");
    writeRenameReadCompare(path, 16 * 1024);
  }

  @Test
  public void testMediumUpload() throws IOException, NoSuchAlgorithmException {
    Path path = new Path("upload/medium.file");
    writeRenameReadCompare(path, 6 * 1024 * 1024);
  }

  @Test
  public void testExtraLargeUpload() throws IOException, NoSuchAlgorithmException {
    conf.setInt(OSS_MULTIPART_UPLOADS_BLOCK_SIZE_PROPERTY, 1024 * 1024);
    fs.initialize(URI.create(conf.get(TEST_URI_PROPERTY)), conf);

    Path path = new Path("upload/large.file");
    writeRenameReadCompare(path, 6 * 1024 * 1024);
  }

  @Test
  public void testMissingFile() throws IOException {
    thrown.expect(FileNotFoundException.class);
    store.retrieve("s/o/m/e/m/i/s/s/i/n/g/f/i/l/e");
  }

  @Test
  public void testMissingBucket() throws IOException {
    thrown.expect(FileNotFoundException.class);
    fs.initialize(URI.create("oss://some-missing-bucket"), conf);
  }

  @Test
  public void testBadAccessID() throws IOException {
    conf.set(ACCESS_KEY_PROPERTY, "xxx");
    thrown.expect(AccessControlException.class);
    fs.initialize(URI.create(conf.get(TEST_URI_PROPERTY)), conf);
  }

  @Test
  public void testMultipartListing() throws Exception {
    final int count = 50;

    conf.setInt(OSS_MAX_LISTING_LENGTH_PROPERTY, 2);
    fs.initialize(URI.create(conf.get(TEST_URI_PROPERTY)), conf);

    Path path = fs.makeQualified(new Path("list"));
    String base = path.toUri().getPath().substring(1);

    for (int i = 0; i < count; i++) {
      store.storeEmptyFile(base + "/x-" + i);
    }

    // check count
    FileStatus[] files = fs.listStatus(path);
    assertEquals(count, files.length);

    // collect all names, check
    TreeSet<String> names = new TreeSet<>();
    for (FileStatus file : files) names.add(file.getPath().getName());
    for (int i = 0; i < count; i++) assertTrue(names.contains("x-" + i));
  }
}
