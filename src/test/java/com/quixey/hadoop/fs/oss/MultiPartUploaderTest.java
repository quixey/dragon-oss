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

import com.aliyun.openservices.oss.OSSClient;
import com.aliyun.openservices.oss.model.CompleteMultipartUploadRequest;
import com.aliyun.openservices.oss.model.InitiateMultipartUploadRequest;
import com.aliyun.openservices.oss.model.InitiateMultipartUploadResult;
import com.aliyun.openservices.oss.model.UploadPartRequest;
import com.aliyun.openservices.oss.model.UploadPartResult;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.Matchers;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.UUID;

import static com.google.common.base.Optional.of;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link com.quixey.hadoop.fs.oss.MultiPartUploader}.
 *
 * @author Jim Lim - jim@quixey.com
 */
public class MultiPartUploaderTest {

  private static final String bucket = "some.bucket";

  private OSSClient         client;
  private MultiPartUploader uploader;

  @Before
  public void setUp() {
    client = mock(OSSClient.class);
    Configuration conf = new Configuration();
    uploader = new MultiPartUploader(client, bucket, conf);
  }

  @Test
  public void testSmallUpload() {
    assertFalse(uploader.shouldUpload(10));
  }

  @Test
  public void testLargeUpload() {
    assertTrue(uploader.shouldUpload(100 * 1024 * 1024));
  }

  @Test
  public void testToString() {
    assertNotNull(uploader.toString());
    assertTrue(0 < uploader.toString().length());
  }

  @Test
  public void testMultipartUpload() throws IOException, NoSuchAlgorithmException {
    uploader.setPartSize(1024 * 1024);

    final long len = 9 * 1024 * 1024 + 10;
    Path file = Files.createTempFile("multipart-upload-", ".bin");
    MessageDigest digest = MessageDigest.getInstance("MD5");
    try (OutputStream out = newOutputStream(file, digest)) {
      for (long i = 0; i < len; i++) out.write(74);
    }

    final String key = "/some/random/key";
    final byte[] md5Hash = digest.digest();
    String uploadId = UUID.randomUUID().toString();
    mockInitiateMultiPartUpload(key, uploadId, md5Hash);
    mockUploadPart();

    uploader.upload(key, file.toFile(), of(md5Hash));

    verify(client, times(1)).initiateMultipartUpload(initiateRequest(key, md5Hash));
    for (int i = 1; i <= 9; i++) {
      verify(client, times(1)).uploadPart(uploadPartRequest(key, uploadId, i, 1024 * 1024));
    }
    verify(client, times(1)).uploadPart(uploadPartRequest(key, uploadId, 10, 10));
    verify(client, times(1)).completeMultipartUpload(completeRequest(key, uploadId));
  }

  private CompleteMultipartUploadRequest completeRequest(final String key, final String uploadId) {
    return argThat(new ArgumentMatcher<CompleteMultipartUploadRequest>() {
      @Override
      public boolean matches(Object argument) {
        if (!(argument instanceof CompleteMultipartUploadRequest)) return false;
        CompleteMultipartUploadRequest request = (CompleteMultipartUploadRequest) argument;
        return bucket.equals(request.getBucketName()) &&
               key.equals(request.getKey()) &&
               uploadId.equals(request.getUploadId());
        // TODO etags
      }
    });
  }

  @Test
  @Ignore
  public void testExceptionHandling() {
    // TODO fill me in
  }

  private void mockInitiateMultiPartUpload(String key, String uploadId, byte[] md5Hash) {
    InitiateMultipartUploadResult result = new InitiateMultipartUploadResult();
    result.setUploadId(uploadId);
    when(client.initiateMultipartUpload(initiateRequest(key, md5Hash))).thenReturn(result);
  }

  private void mockUploadPart() {
    UploadPartResult result = new UploadPartResult();
    result.setETag(UUID.randomUUID().toString());
    when(client.uploadPart(Matchers.<UploadPartRequest>any())).thenReturn(result);
  }

  private UploadPartRequest uploadPartRequest(final String key,
                                              final String uploadId,
                                              final int partNum,
                                              final int size) {
    return argThat(new ArgumentMatcher<UploadPartRequest>() {
      @Override
      public boolean matches(Object argument) {
        if (!(argument instanceof UploadPartRequest)) return false;
        UploadPartRequest request = (UploadPartRequest) argument;
        return bucket.equals(request.getBucketName()) &&
               key.equals(request.getKey()) &&
               uploadId.equals(request.getUploadId()) &&
               partNum == request.getPartNumber() &&
               size == request.getPartSize();
      }
    });
  }

  private InitiateMultipartUploadRequest initiateRequest(final String key, final byte[] md5Hash) {
    final String expectedMd5 = Base64.encodeBase64String(md5Hash);
    return argThat(new ArgumentMatcher<InitiateMultipartUploadRequest>() {
      @Override
      public boolean matches(Object argument) {
        if (!(argument instanceof InitiateMultipartUploadRequest)) return false;
        InitiateMultipartUploadRequest request = (InitiateMultipartUploadRequest) argument;
        String actualMd5 = (String) request.getObjectMetadata().getRawMetadata().get("Content-MD5");
        return bucket.equals(request.getBucketName()) &&
               key.equals(request.getKey()) &&
               expectedMd5.trim().equals(actualMd5);
      }
    });
  }

  private BufferedOutputStream newOutputStream(Path file, MessageDigest digest) throws IOException {
    return new BufferedOutputStream(new DigestOutputStream(Files.newOutputStream(file), digest));
  }
}