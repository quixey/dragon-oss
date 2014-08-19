package com.quixey.hadoop.fs.oss;

import com.google.common.base.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Time;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import static com.quixey.hadoop.fs.oss.OSSFileSystemConfigKeys.OSS_BUFFER_DIR_PROPERTY;

/**
 * <p>
 * A stub implementation of {@link com.quixey.hadoop.fs.oss.FileSystemStore} for testing
 * {@link com.quixey.hadoop.fs.oss.OSSFileSystem} without actually connecting to OSS. Copied from S3FileSystem.
 * </p>
 */
public class InMemoryFileSystemStore implements FileSystemStore {

  private Configuration conf;

  private SortedMap<String, FileMetadata> metadataMap = new TreeMap<>();
  private SortedMap<String, byte[]>       dataMap     = new TreeMap<>();

  @Override
  public void initialize(URI uri, Configuration conf) throws IOException {
    this.conf = conf;
  }

  @Override
  public void storeFile(String key, File file, Optional<byte[]> md5Hash) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    byte[] buf = new byte[8192];
    int numRead;
    BufferedInputStream in = null;
    try {
      in = new BufferedInputStream(new FileInputStream(file));
      while ((numRead = in.read(buf)) >= 0) {
        out.write(buf, 0, numRead);
      }
    } finally {
      if (null != in) in.close();
    }
    metadataMap.put(key, new FileMetadata(key, file.length(), Time.now()));
    dataMap.put(key, out.toByteArray());
  }

  @Override
  public void storeEmptyFile(String key) throws IOException {
    metadataMap.put(key, new FileMetadata(key, 0, Time.now()));
    dataMap.put(key, new byte[0]);
  }

  @Override
  public FileMetadata retrieveMetadata(String key) throws IOException {
    return metadataMap.get(key);
  }

  @Override
  @Nonnull
  public InputStream retrieve(String key) throws IOException {
    return retrieve(key, 0);
  }

  @Override
  @Nonnull
  public InputStream retrieve(String key, long byteRangeStart) throws IOException {

    byte[] data = dataMap.get(key);
    File file = createTempFile();
    BufferedOutputStream out = null;
    try {
      out = new BufferedOutputStream(new FileOutputStream(file));
      out.write(data, (int) byteRangeStart, data.length - (int) byteRangeStart);
    } finally {
      if (null != out) out.close();
    }
    return new FileInputStream(file);
  }

  @Override
  @Nonnull
  public PartialListing list(String prefix, int maxListingLength) throws IOException {
    return list(prefix, maxListingLength, null, false);
  }

  @Override
  @Nonnull
  public PartialListing list(String prefix,
                             int maxListingLength,
                             @Nullable String marker,
                             boolean recursive) throws IOException {
    // we are not using marker here, because #list always returns everything
    return list(prefix, recursive ? null : PATH_DELIMITER, maxListingLength);
  }

  @Override
  public void delete(String key) throws IOException {
    metadataMap.remove(key);
    dataMap.remove(key);
  }

  @Override
  public void copy(String srcKey, String dstKey) throws IOException {
    metadataMap.put(dstKey, metadataMap.get(srcKey));
    dataMap.put(dstKey, dataMap.get(srcKey));
  }

  @Override
  public void purge(String prefix) throws IOException {
    Iterator<Entry<String, FileMetadata>> i =
        metadataMap.entrySet().iterator();
    while (i.hasNext()) {
      Entry<String, FileMetadata> entry = i.next();
      if (entry.getKey().startsWith(prefix)) {
        dataMap.remove(entry.getKey());
        i.remove();
      }
    }
  }

  private File createTempFile() throws IOException {
    File dir = new File(conf.get(OSS_BUFFER_DIR_PROPERTY));
    if (!dir.exists() && !dir.mkdirs()) {
      throw new IOException("Cannot create OSS buffer directory: " + dir);
    }
    File result = File.createTempFile("test-", ".tmp", dir);
    result.deleteOnExit();
    return result;
  }

  private PartialListing list(String prefix,
                              @Nullable String delimiter,
                              int maxListingLength) throws IOException {

    if (prefix.length() > 0 && !prefix.endsWith(PATH_DELIMITER)) {
      prefix += PATH_DELIMITER;
    }

    List<FileMetadata> metadata = new ArrayList<>();
    SortedSet<String> commonPrefixes = new TreeSet<>();
    for (String key : dataMap.keySet()) {
      if (key.startsWith(prefix)) {
        if (null == delimiter) {
          metadata.add(retrieveMetadata(key));
        } else {
          int delimIndex = key.indexOf(delimiter, prefix.length());
          if (delimIndex == -1) {
            metadata.add(retrieveMetadata(key));
          } else {
            String commonPrefix = key.substring(0, delimIndex);
            commonPrefixes.add(commonPrefix);
          }
        }
      }
      int mdSize = metadata.size();
      int cpSize = commonPrefixes.size();
      if (metadata.size() + commonPrefixes.size() == maxListingLength) {
        new PartialListing(key, metadata.toArray(new FileMetadata[mdSize]), commonPrefixes.toArray(new String[cpSize]));
      }
    }

    int mdSize = metadata.size();
    int cpSize = commonPrefixes.size();
    return new PartialListing(null, metadata.toArray(new FileMetadata[mdSize]), commonPrefixes.toArray(new String[cpSize]));
  }
}
