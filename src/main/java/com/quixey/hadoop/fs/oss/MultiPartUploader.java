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
import com.aliyun.openservices.oss.model.ObjectMetadata;
import com.aliyun.openservices.oss.model.PartETag;
import com.aliyun.openservices.oss.model.UploadPartRequest;
import com.aliyun.openservices.oss.model.UploadPartResult;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.io.ByteStreams;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.quixey.hadoop.fs.oss.OSSFileSystemConfigKeys.OSS_MULTIPART_UPLOADS_BLOCK_SIZE_PROPERTY;
import static com.quixey.hadoop.fs.oss.OSSFileSystemConfigKeys.OSS_MULTIPART_UPLOADS_MAX_THREADS_PROPERTY;

/**
 * Handles multipart upload for OSS.
 *
 * @author Jim Lim - jim@quixey.com
 */
@VisibleForTesting
class MultiPartUploader {

  public static final  long   MAX_PART_SIZE                = (long) 5 * 1024 * 1024 * 1024;
  public static final  int    DEFAULT_MULTIPART_BLOCK_SIZE = 64 * 1024 * 1024;
  private static final Logger LOG                          = LoggerFactory.getLogger(CloudOSSFileSystemStore.class);
  private final OSSClient client;
  private final String    bucket;
  private final boolean   multipartEnabled;
  private final int       maxThreads;

  private long partSize;

  MultiPartUploader(OSSClient client, String bucket, Configuration conf) {
    this.client = checkNotNull(client);
    this.bucket = checkNotNull(bucket);

    checkNotNull(conf);

    multipartEnabled = conf.getBoolean(OSSFileSystemConfigKeys.OSS_MULTIPART_UPLOADS_ENABLED, true);
    partSize = conf.getLong(OSS_MULTIPART_UPLOADS_BLOCK_SIZE_PROPERTY, DEFAULT_MULTIPART_BLOCK_SIZE);
    maxThreads = conf.getInt(OSS_MULTIPART_UPLOADS_MAX_THREADS_PROPERTY, Integer.MAX_VALUE);

    checkArgument(partSize <= MAX_PART_SIZE, "%s must be at most %s", OSS_MULTIPART_UPLOADS_BLOCK_SIZE_PROPERTY, MAX_PART_SIZE);
  }

  /**
   * @param fileSize size of local file
   * @return true iff {@code file} should be broken up into parts and uploaded
   */
  boolean shouldUpload(long fileSize) {
    return multipartEnabled && partSize < fileSize;
  }

  /**
   * Breaks up {@code file} into multiple parts and uploads it.
   *
   * @param key     destination key
   * @param file    local file
   * @param md5Hash checksum
   * @throws com.aliyun.openservices.ServiceException from OSSClient.
   */
  void upload(String key, File file, Optional<byte[]> md5Hash) throws IOException {
    checkNotNull(key);
    checkNotNull(file);
    checkNotNull(md5Hash);

    // initiate upload
    ObjectMetadata metadata = metadata(file, md5Hash);
    String uploadId = initiateMultiPartUpload(key, metadata);

    // count number of parts
    int parts = (int) calculateNumParts(file.length());
    LOG.info("Initiating multipart upload request for key {} with {} parts", key, parts);

    List<PartETag> eTags = uploadParts(key, file, uploadId, parts);
    completeMultipartUpload(key, uploadId, eTags);
  }

  @VisibleForTesting
  void setPartSize(long partSize) {
    this.partSize = partSize;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
                  .add("bucket", bucket)
                  .add("multipart enabled", multipartEnabled)
                  .add("part size", partSize)
                  .add("max threads", maxThreads)
                  .toString();
  }

  private void completeMultipartUpload(String key, String uploadId, List<PartETag> eTags) {
    CompleteMultipartUploadRequest request = new CompleteMultipartUploadRequest(bucket, key, uploadId, eTags);
    client.completeMultipartUpload(request);
  }

  @SuppressWarnings("unchecked")
  private List<PartETag> uploadParts(final String key,
                                     final File file,
                                     final String uploadId,
                                     int parts) throws IOException {

    // construct thread pool
    ExecutorService pool = newExecutorService(file, parts);

    final Future<PartETag>[] futures = new Future[parts];
    for (int i = 0; i < parts; i++) {
      final int partNum = i;
      futures[i] = pool.submit(new Callable<PartETag>() {
        @Override
        public PartETag call() throws Exception {
          return uploadPart(key, file, uploadId, partNum);
        }
      });
    }
    pool.shutdown();

    // wait for uploads to complete
    awaitTermination(pool);

    // retrieve etags and verify uploads
    PartETag[] eTags = new PartETag[parts];
    int i = 0;
    for (Future<PartETag> future : futures) {
      try {
        eTags[i++] = future.get();
      } catch (InterruptedException | ExecutionException e) {
        throw new IOException("Unable to upload part " + i, e);
      }
    }

    return Arrays.asList(eTags);
  }

  private void awaitTermination(ExecutorService pool) {
    while (!pool.isTerminated()) {
      try {
        pool.awaitTermination(5, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private ExecutorService newExecutorService(File file, int parts) {
    int threads = Math.min(parts, maxThreads);
    ThreadFactory factory = new ThreadFactoryBuilder().setNameFormat("dragon-mp-" + file.getName() + "-%d").build();
    return Executors.newFixedThreadPool(threads, factory);
  }

  /**
   * Uploads a single part to OSS.
   *
   * @param key      destination key
   * @param file     local file
   * @param uploadId upload ID
   * @param partNum  part number
   * @return part etag
   */
  private PartETag uploadPart(String key, File file, String uploadId, int partNum) {
    long start = partNum * partSize;
    long size = Math.min(partSize, file.length() - start);

    try (InputStream stream = new BufferedInputStream(new FileInputStream(file))) {

      if (start > 0) ByteStreams.skipFully(stream, start);

      UploadPartRequest request = new UploadPartRequest();
      request.setBucketName(bucket);
      request.setInputStream(stream);
      request.setKey(key);
      request.setPartNumber(partNum + 1);
      request.setPartSize(size);
      request.setUploadId(uploadId);

      UploadPartResult result = client.uploadPart(request);
      LOG.info("{} part {}: upload complete", key, partNum);

      return result.getPartETag();
    } catch (IOException e) {
      throw new RuntimeException("Error uploading part " + partNum + " of " + key, e);
    }
  }

  /**
   * @param length size of the local file
   * @return number of parts to upload
   */
  private long calculateNumParts(long length) {
    return length / partSize + (0 == length % partSize ? 0 : 1);
  }

  private String initiateMultiPartUpload(String key, ObjectMetadata metadata) {
    InitiateMultipartUploadRequest request = new InitiateMultipartUploadRequest(bucket, key);
    request.setObjectMetadata(metadata);
    InitiateMultipartUploadResult result = client.initiateMultipartUpload(request);
    return result.getUploadId();
  }

  /**
   * @param file    file to upload
   * @param md5Hash file checksum
   * @return object metadata
   */
  private ObjectMetadata metadata(File file, Optional<byte[]> md5Hash) {
    // create metadata
    ObjectMetadata metadata = new ObjectMetadata();
    metadata.setContentLength(file.length());
    metadata.setLastModified(new Date(file.lastModified()));
    // add MD5, if provided
    if (md5Hash.isPresent()) {
      String contentMd5 = Base64.encodeBase64String(md5Hash.get());
      metadata.setHeader("Content-MD5", contentMd5.trim());
    }
    return metadata;
  }
}
