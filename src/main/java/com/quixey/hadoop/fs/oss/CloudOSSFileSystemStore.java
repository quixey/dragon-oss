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

import com.aliyun.openservices.ServiceException;
import com.aliyun.openservices.oss.OSSClient;
import com.aliyun.openservices.oss.OSSErrorCode;
import com.aliyun.openservices.oss.OSSException;
import com.aliyun.openservices.oss.model.GetObjectRequest;
import com.aliyun.openservices.oss.model.ListObjectsRequest;
import com.aliyun.openservices.oss.model.OSSObject;
import com.aliyun.openservices.oss.model.OSSObjectSummary;
import com.aliyun.openservices.oss.model.ObjectListing;
import com.aliyun.openservices.oss.model.ObjectMetadata;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.AccessControlException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Date;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Implements {@link FileSystemStore} using actual OSS api calls.
 *
 * @author Jim Lim - jim@quixey.com
 */
@VisibleForTesting
class CloudOSSFileSystemStore implements FileSystemStore {

  private static final Logger LOG = LoggerFactory.getLogger(CloudOSSFileSystemStore.class);

  private OSSClient client;
  private String    bucket;

  private MultiPartUploader multiPartUploader;

  @Override
  public void initialize(URI uri, Configuration conf) throws IOException {
    uri = checkNotNull(uri);
    conf = checkNotNull(conf);

    OSSCredentials ossCredentials = new OSSCredentials();
    ossCredentials.initialize(uri, conf);

    client = new OSSClient(ossCredentials.getAccessKeyId(), ossCredentials.getSecretAccessKey());
    bucket = checkNotNull(uri.getHost(), "URI must include a host: %s", uri);

    // TODO deal with server-side encryption for store, copy
    multiPartUploader = new MultiPartUploader(client, bucket, conf);
  }

  @Override
  public void storeFile(String key, File file, Optional<byte[]> md5Hash) throws IOException {
    checkNotNull(key);
    checkNotNull(file);
    checkNotNull(md5Hash);

    if (multiPartUploader.shouldUpload(file.length())) {
      doMultipartUpload(key, file, md5Hash);
    } else {
      doSimpleUpload(key, file, md5Hash);
    }
  }

  @Override
  public void storeEmptyFile(String key) throws IOException {
    checkNotNull(key);

    try (InputStream input = new ByteArrayInputStream(new byte[0])) {
      ObjectMetadata metadata = new ObjectMetadata();
      metadata.setContentType("binary/octet-stream");
      metadata.setContentLength(0);
      client.putObject(bucket, key, input, metadata);
    } catch (ServiceException e) {
      throw handleException(e, key);
    }
  }

  @Override
  @Nullable
  public FileMetadata retrieveMetadata(String key) throws IOException {
    checkNotNull(key);

    try {
      ObjectMetadata metadata = client.getObjectMetadata(bucket, key);
      return new FileMetadata(key, metadata.getContentLength(), metadata.getLastModified().getTime());
    } catch (OSSException e) {
      try {
        throw handleException(e, key);
      } catch (FileNotFoundException e2) {
        LOG.debug("{} does not exist", key);
        return null; // degrade gracefully
      }
    }
  }

  @Override
  @Nonnull
  public InputStream retrieve(String key) throws IOException {
    checkNotNull(key);

    try {
      OSSObject object = client.getObject(bucket, key);
      return object.getObjectContent();
    } catch (ServiceException e) {
      throw handleException(e, key);
    }
  }

  @Override
  @Nonnull
  public InputStream retrieve(String key, long byteRangeStart) throws IOException {
    checkNotNull(key);
    try {
      GetObjectRequest request = new GetObjectRequest(bucket, key);
      request.setRange(byteRangeStart, -1);
      OSSObject object = client.getObject(request);
      return object.getObjectContent();
    } catch (ServiceException e) {
      throw handleException(e, key);
    }
  }

  @Override
  @Nonnull
  public PartialListing list(String prefix, int maxListingLength) throws IOException {
    checkNotNull(prefix);
    return list(prefix, maxListingLength, null, false);
  }

  @Override
  @Nonnull
  public PartialListing list(String prefix,
                             int maxListingLength,
                             @Nullable String marker,
                             boolean recursive) throws IOException {
    checkNotNull(prefix);
    return list(prefix, recursive ? null : PATH_DELIMITER, maxListingLength, marker);
  }

  @Override
  public void delete(String key) throws IOException {
    checkNotNull(key);
    LOG.debug("Deleting key: {} from bucket: {}", key, bucket);
    try {
      client.deleteObject(bucket, key);
    } catch (ServiceException e) {
      throw handleException(e, key);
    }
  }

  @Override
  public void copy(String srcKey, String dstKey) throws IOException {
    checkNotNull(srcKey);
    checkNotNull(dstKey);
    LOG.debug("Copying srcKey: {} to dstKey: {} in bucket: {}", srcKey, dstKey, bucket);
    try {
      client.copyObject(bucket, srcKey, bucket, dstKey);
    } catch (ServiceException e) {
      throw handleException(e, srcKey);
    }
  }

  @Override
  public void purge(String prefix) throws IOException {
    checkNotNull(prefix);
    try {
      ObjectListing objects = client.listObjects(bucket, prefix);
      for (OSSObjectSummary summary : objects.getObjectSummaries()) {
        String key = summary.getKey();
        client.deleteObject(bucket, key);
      }
    } catch (ServiceException e) {
      throw handleException(e, prefix);
    }
  }

  private void doMultipartUpload(String key, File file, Optional<byte[]> md5Hash) throws IOException {
    try {
      multiPartUploader.upload(key, file, md5Hash);
    } catch (ServiceException e) {
      throw handleException(e, key);
    }
  }

  private void doSimpleUpload(String key, File file, Optional<byte[]> md5Hash) throws IOException {
    try (BufferedInputStream input = new BufferedInputStream(new FileInputStream(file))) {
      ObjectMetadata metadata = new ObjectMetadata();
      metadata.setContentLength(file.length());
      metadata.setLastModified(new Date(file.lastModified()));
      // add MD5, if provided
      if (md5Hash.isPresent()) {
        String contentMd5 = Base64.encodeBase64String(md5Hash.get());
        metadata.setHeader("Content-MD5", contentMd5.trim());
      }
      client.putObject(bucket, key, input, metadata);
    } catch (ServiceException e) {
      throw handleException(e, key);
    }
  }

  /**
   * List objects.
   *
   * @param prefix           prefix
   * @param delimiter        delimiter
   * @param maxListingLength max no. of entries
   * @param marker           last key in any previous search
   * @return a list of matches
   * @throws IOException on any reported failure
   */
  private PartialListing list(String prefix,
                              @Nullable String delimiter,
                              int maxListingLength,
                              @Nullable String marker) throws IOException {
    if (!prefix.isEmpty() && !prefix.endsWith(PATH_DELIMITER)) {
      prefix += PATH_DELIMITER;
    }

    ListObjectsRequest request = new ListObjectsRequest(bucket, prefix, marker, delimiter, maxListingLength);

    // make api request
    ObjectListing chunk;
    try {
      chunk = client.listObjects(request);
    } catch (ServiceException e) {
      throw handleException(e, prefix);
    }

    // convert object summary to file metadata
    List<OSSObjectSummary> summaries = chunk.getObjectSummaries();
    FileMetadata[] fileMetadata = new FileMetadata[summaries.size()];
    int i = 0;
    for (OSSObjectSummary summary : summaries) {
      fileMetadata[i++] = new FileMetadata(summary.getKey(), summary.getSize(), summary.getLastModified().getTime());
    }

    // construct partial listing
    List<String> commonPrefixes = chunk.getCommonPrefixes();
    String[] prefixesArray = commonPrefixes.toArray(new String[commonPrefixes.size()]);
    return new PartialListing(chunk.getNextMarker(), fileMetadata, prefixesArray);
  }

  /**
   * Translates Aliyun Exceptions into Hadoop-compatible exceptions.
   *
   * @param thrown Service Exception
   * @param key    associated OSS key
   * @return exception
   */
  private IOException handleException(ServiceException thrown, String key) {
    LOG.debug("ServiceException encountered.", thrown);
    switch (thrown.getErrorCode()) {
      case OSSErrorCode.NO_SUCH_KEY:
        return new FileNotFoundException(key);
      case OSSErrorCode.NO_SUCH_BUCKET:
        return new FileNotFoundException(bucket);
      case OSSErrorCode.ACCESS_DENIED:
      case OSSErrorCode.INVALID_ACCESS_KEY_ID:
        return new AccessControlException("bucket: " + bucket + ", key: " + key);
      default:
        return new IOException(thrown);
    }
  }
}
