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
import com.google.common.base.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BufferedFSInputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.BufferedOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Optional.absent;
import static com.google.common.base.Optional.fromNullable;
import static com.google.common.base.Optional.of;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.quixey.hadoop.fs.oss.OSSFileSystemConfigKeys.OSS_BLOCK_SIZE_PROPERTY;
import static com.quixey.hadoop.fs.oss.OSSFileSystemConfigKeys.OSS_BUFFER_DIR_PROPERTY;
import static com.quixey.hadoop.fs.oss.OSSFileSystemConfigKeys.OSS_MAX_LISTING_LENGTH_PROPERTY;
import static com.quixey.hadoop.fs.oss.OSSFileSystemConfigKeys.OSS_MAX_RETRIES_PROPERTY;
import static com.quixey.hadoop.fs.oss.OSSFileSystemConfigKeys.OSS_SLEEP_TIME_SECONDS_PROPERTY;

// TODO convert ls, delete, and rename to use an iterator

/**
 * <p>
 * A {@link org.apache.hadoop.fs.FileSystem} for reading and writing files stored on
 * <a href="http://www.aliyun.com/product/oss/">Aliyun OSS</a>.
 * </p>
 *
 * @author Jim Lim - jim@quixey.com
 */
public class OSSFileSystem extends FileSystem {

  public static final  Logger LOG                = LoggerFactory.getLogger(OSSFileSystem.class);
  public static final  int    DEFAULT_BLOCK_SIZE = 64 * 1024 * 1024;
  private static final String FOLDER_SUFFIX      = "/";
  private int             maxListingLength;
  private URI             uri;
  private FileSystemStore store;
  private Path            workingDir;

  @SuppressWarnings("unused")
  public OSSFileSystem() {
    // set store in #initialize
  }

  public OSSFileSystem(FileSystemStore store) {
    this.store = checkNotNull(store);
  }

  private static FileSystemStore createDefaultStore(Configuration conf) {
    FileSystemStore store = new CloudOSSFileSystemStore();

    RetryPolicy basePolicy = RetryPolicies.retryUpToMaximumCountWithFixedSleep(
        conf.getInt(OSS_MAX_RETRIES_PROPERTY, 4),
        conf.getLong(OSS_SLEEP_TIME_SECONDS_PROPERTY, 10), TimeUnit.SECONDS);
    Map<Class<? extends Exception>, RetryPolicy> exceptionToPolicyMap = new HashMap<>();
    exceptionToPolicyMap.put(IOException.class, basePolicy);

    RetryPolicy methodPolicy = RetryPolicies.retryByException(
        RetryPolicies.TRY_ONCE_THEN_FAIL, exceptionToPolicyMap);
    Map<String, RetryPolicy> methodNameToPolicyMap = new HashMap<>();
    methodNameToPolicyMap.put("storeFile", methodPolicy);
    methodNameToPolicyMap.put("rename", methodPolicy);

    return (FileSystemStore) RetryProxy.create(FileSystemStore.class, store, methodNameToPolicyMap);
  }

  /**
   * Constructs a OSS key from given {@code path}.
   *
   * If a path has a trailing slash, it's removed.
   *
   * @param path absolute HDFS path
   * @return OSS key
   */
  private static String pathToKey(Path path) {
    if (null != path.toUri().getScheme() && path.toUri().getPath().isEmpty()) {
      // allow uris without trailing slash after bucket to refer to root,
      // like oss://mybucket
      return "";
    }

    checkArgument(path.isAbsolute(), "Path must be absolute: " + path);

    String ret = path.toUri().getPath().substring(1); // remove initial slash
    if (ret.endsWith("/") && ret.indexOf("/") != ret.length() - 1) {
      ret = ret.substring(0, ret.length() - 1);
    }

    return ret;
  }

  /**
   * Constructs a HDFS path from given {@code key}.
   *
   * @param key OSS key
   * @return HDFS path
   */
  private static Path keyToPath(String key) {
    return new Path("/" + key);
  }

  @Override
  public void initialize(URI uri, Configuration conf) throws IOException {
    super.initialize(checkNotNull(uri), checkNotNull(conf));
    if (null == store) store = createDefaultStore(conf);
    store.initialize(uri, conf);
    setConf(conf);
    this.uri = URI.create(uri.getScheme() + "://" + uri.getAuthority());
    this.workingDir = makeQualified(new Path("/user", System.getProperty("user.name")));
    this.maxListingLength = conf.getInt(OSS_MAX_LISTING_LENGTH_PROPERTY, 1000);
  }

  @Override
  @Nonnull
  public String getScheme() {
    return "oss";
  }

  @Override
  @Nullable
  public URI getUri() {
    return uri; // schema + authority, used for identifying the filesystem e.g. oss://abc
  }

  @Override
  @Nullable
  public String getCanonicalServiceName() {
    // Does not support Token
    return null;
  }

  @Override
  @Nonnull
  public FSDataInputStream open(Path path, int bufferSize) throws IOException {
    path = checkNotNull(path);
    FileStatus fs = getFileStatus(path); // will throw if the file doesn't exist

    if (fs.isDirectory()) throw new FileNotFoundException("'" + path + "' is a directory");
    LOG.info("Opening '{}' for reading", path);

    Path absolutePath = makeAbsolute(path);
    String key = pathToKey(absolutePath);
    return new FSDataInputStream(new BufferedFSInputStream(new OSSFileInputStream(store, key, of(statistics)), bufferSize));
  }

  @Override
  @Nonnull
  public FSDataOutputStream create(Path path,
                                   FsPermission permission,
                                   boolean overwrite,
                                   int bufferSize,
                                   short replication,
                                   long blockSize,
                                   @Nullable Progressable progress) throws IOException {
    path = checkNotNull(path);
    if (exists(path) && !overwrite) {
      throw new FileAlreadyExistsException("File already exists: " + path);
    }

    LOG.debug("Creating new file '{}' in OSS", path);
    Path absolutePath = makeAbsolute(path);
    String key = pathToKey(absolutePath);
    return new FSDataOutputStream(new OSSFileOutputStream(getConf(), key, fromNullable(progress)), statistics);
  }

  @Override
  public FSDataOutputStream append(Path f, int bufferSize, Progressable progress) throws IOException {
    throw new IOException("#append is not supported by OSSFileSystem.");
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    src = checkNotNull(src);
    dst = checkNotNull(dst);

    String srcKey = pathToKey(makeAbsolute(src));
    final String debugPreamble = "Renaming '" + src + "' to '" + dst + "' - ";

    if (isRoot(srcKey)) {
      // Cannot rename root of file system
      LOG.debug("{} returning false as cannot rename the root of a filesystem", debugPreamble);
      return false;
    }

    // get status of source
    boolean srcIsFile;
    try {
      srcIsFile = getFileStatus(src).isFile();
    } catch (FileNotFoundException e) {
      // bail out fast if the source does not exist
      LOG.debug("{} returning false as src does not exist", debugPreamble);
      return false;
    }

    // figure out the final destination
    String dstKey = pathToKey(makeAbsolute(dst));

    try {
      boolean dstIsFile = getFileStatus(dst).isFile();
      if (dstIsFile) {
        // destination is a file.
        // you can't copy a file or a directory onto an existing file
        // except for the special case of dest==src, which is a no-op
        LOG.debug("{} returning without rename as dst is an already existing file", debugPreamble);
        // exit, returning true iff the rename is onto self
        return srcKey.equals(dstKey);
      } else {
        // destination exists and is a directory
        LOG.debug("{} using dst as output directory", debugPreamble);
        // destination goes under the dst path, with the name of the
        // source entry
        dstKey = pathToKey(makeAbsolute(new Path(dst, src.getName())));
      }
    } catch (FileNotFoundException e) {
      // destination does not exist => the source file or directory
      // is copied over with the name of the destination
      LOG.debug("{} using dst as output destination", debugPreamble);
      try {
        if (getFileStatus(dst.getParent()).isFile()) {
          LOG.debug("{} returning false as dst parent exists and is a file", debugPreamble);
          return false;
        }
      } catch (FileNotFoundException ex) {
        LOG.debug("{} returning false as dst parent does not exist", debugPreamble);
        return false;
      }
    }

    // rename to self behavior follows Posix rules and is different
    // for directories and files -the return code is driven by src type
    if (srcKey.equals(dstKey)) {
      // fully resolved destination key matches source: fail
      LOG.debug("{} renamingToSelf; returning true", debugPreamble);
      return true;
    }

    if (srcIsFile) {
      renameOneFile(srcKey, dstKey, debugPreamble);
    } else {
      // src is a directory
      LOG.debug("{} src is directory, so copying contents", debugPreamble);

      // verify dest is not a child of the parent
      if (isSubDir(dstKey, srcKey)) {
        LOG.debug("{} cannot rename a directory to a subdirectory of self", debugPreamble);
        return false;
      }

      renameDir(srcKey, dstKey, debugPreamble);
      LOG.debug("{} done", debugPreamble);
    }

    return true;
  }

  @Override
  public boolean delete(Path path, boolean recursive) throws IOException {
    path = checkNotNull(path);

    FileStatus status;
    try {
      status = getFileStatus(path);
    } catch (FileNotFoundException e) {
      LOG.debug("Delete called for '{}', but file does not exist, so returning false.", path);
      return false;
    }

    Path absolutePath = makeAbsolute(path);
    String key = pathToKey(absolutePath);

    if (status.isDirectory()) {
      if (!recursive && listStatus(path).length > 0) {
        throw new IOException("Cannot delete '" + path + "' as it is not an empty directory and recurse option is false");
      }

      // recursively delete directory
      createParent(path);

      LOG.debug("Deleting directory '{}'", path);
      String marker = null;
      do {
        PartialListing listing = store.list(key, maxListingLength, marker, true);
        for (FileMetadata file : listing.getFiles()) store.delete(file.getKey());
        marker = listing.getMarker();
      } while (null != marker);

      rmdir(key);
    } else {
      LOG.debug("Deleting file '{}'", path);
      createParent(path);
      store.delete(key);
    }

    return true;
  }

  @Override
  @Nonnull
  public FileStatus[] listStatus(Path path) throws IOException {
    path = checkNotNull(path);

    Path absolutePath = makeAbsolute(path);
    String key = pathToKey(absolutePath);

    if (key.length() > 0) {
      FileMetadata meta = store.retrieveMetadata(key);
      // if metadata exists, return it
      if (null != meta) {
        return new FileStatus[]{newFile(meta, absolutePath)};
      }
    }

    // treat path as a directory, and collect its children
    URI pathUri = absolutePath.toUri();
    Set<FileStatus> status = new TreeSet<>();
    String marker = null;
    do {
      PartialListing listing = store.list(key, maxListingLength, marker, false);

      for (FileMetadata fileMetadata : listing.getFiles()) {
        Path subpath = keyToPath(fileMetadata.getKey());
        String relativePath = pathUri.relativize(subpath.toUri()).getPath();

        if (fileMetadata.getKey().equals(key + "/")) {
          // this is just the directory we have been asked to list
          LOG.trace("..");
        } else if (relativePath.endsWith(FOLDER_SUFFIX)) {
          status.add(newDirectory(new Path(
              absolutePath,
              relativePath.substring(0, relativePath.indexOf(FOLDER_SUFFIX)))));
        } else {
          status.add(newFile(fileMetadata, subpath));
        }
      }

      for (String commonPrefix : listing.getCommonPrefixes()) {
        Path subpath = keyToPath(commonPrefix);
        String relativePath = pathUri.relativize(subpath.toUri()).getPath();
        status.add(newDirectory(new Path(absolutePath, relativePath)));
      }

      // keep paging through the listing
      marker = listing.getMarker();
    } while (null != marker);

    if (status.isEmpty() &&
        key.length() > 0 &&
        null == store.retrieveMetadata(key + FOLDER_SUFFIX)) {
      throw new FileNotFoundException("File " + path + " does not exist.");
    }

    return status.toArray(new FileStatus[status.size()]);
  }

  private void renameDir(String srcKey, String dstKey, String preamble) throws IOException {
    // create the subdir under the destination
    store.storeEmptyFile(dstKey + FOLDER_SUFFIX);

    List<String> keysToDelete = new ArrayList<>();
    String marker = null;
    do {
      PartialListing listing = store.list(srcKey, maxListingLength, marker, true);
      for (FileMetadata file : listing.getFiles()) {
        keysToDelete.add(file.getKey());
        store.copy(file.getKey(), dstKey + file.getKey().substring(srcKey.length()));
      }
      marker = listing.getMarker();
    } while (null != marker);

    LOG.debug("{} all files in src copied, now removing src files", preamble);
    delete(keysToDelete);

    rmdir(srcKey);
  }

  private boolean isSubDir(String child, String parent) {
    return child.startsWith(parent + "/");
  }

  /**
   * Copies file at {@code srcKey} to {@code dstKey}, then deletes {@code srcKey}.
   *
   * @throws IOException
   */
  private void renameOneFile(String srcKey, String dstKey, String preamble) throws IOException {
    LOG.debug("{} src is file, so doing copy then delete in OSS", preamble);
    store.copy(srcKey, dstKey);
    store.delete(srcKey);
  }

  private void rmdir(String key) throws IOException {
    try {
      store.delete(key + FOLDER_SUFFIX);
    } catch (FileNotFoundException e) {
      LOG.trace("Ignoring missing directory: {}", key + FOLDER_SUFFIX);
    }
  }

  private void delete(List<String> keysToDelete) throws IOException {
    for (String key : keysToDelete) store.delete(key);
  }

  private boolean isRoot(String key) {
    return 0 == key.length();
  }

  /**
   * rename() and delete() use this method to ensure that the parent directory of the source does not vanish.
   */
  private void createParent(Path path) throws IOException {
    Path parent = path.getParent();
    if (null != parent) {
      String key = pathToKey(makeAbsolute(parent));
      if (0 < key.length()) store.storeEmptyFile(key + FOLDER_SUFFIX);
    }
  }

  private FileStatus newFile(FileMetadata meta, Path path) {
    final boolean isDir = false;
    final int blockReplication = 1;
    final long blockSize = getDefaultBlockSize(path);
    return new FileStatus(meta.getLength(),
        isDir,
        blockReplication,
        blockSize,
        meta.getLastModified(),
        makeQualified(path));
  }  @Override
  @Nonnull
  public FileStatus getFileStatus(Path path) throws IOException {
    path = checkNotNull(path);
    Path absolutePath = makeAbsolute(path);
    String key = pathToKey(absolutePath);

    // root always exists
    if (isRoot(key)) return newDirectory(absolutePath);

    LOG.debug("getFileStatus retrieving metadata for key '{}'", key);

    FileMetadata meta = store.retrieveMetadata(key);
    if (null != meta) {
      // key exists, file found
      LOG.debug("getFileStatus returning 'file' for key '{}'", key);
      return newFile(meta, absolutePath);
    }

    if (null != store.retrieveMetadata(key + FOLDER_SUFFIX)) {
      // key exists, directory found
      LOG.debug("getFileStatus returning 'directory' for key '{}' as '{}' exists", key, key + FOLDER_SUFFIX);
      return newDirectory(absolutePath);
    }

    LOG.debug("getFileStatus listing key '{}'", key);

    // at this point, we are not sure if it's a file or dir, so just use prefix search
    PartialListing listing = store.list(key, 1);
    if (listing.getFiles().length > 0 ||
        listing.getCommonPrefixes().length > 0) {
      LOG.debug("getFileStatus returning 'directory' for key '{}' as it has contents", key);
      return newDirectory(absolutePath);
    }

    LOG.debug("getFileStatus could not find key '{}'", key);
    throw new FileNotFoundException("No such file or directory '" + absolutePath + "'");
  }

  private FileStatus newDirectory(Path path) {
    final boolean isDir = true;
    final int blockReplication = 1;
    final long blockSize = 0;
    final long lastModified = 0;
    return new FileStatus(0,
        isDir,
        blockReplication,
        blockSize,
        lastModified,
        makeQualified(path));
  }

  /**
   * @return absolute path, from {@code path}.
   */
  private Path makeAbsolute(Path path) {
    if (path.isAbsolute()) return path;
    // assume that all non-absolute paths are relative to the working directory
    return new Path(workingDir, path);
  }

  /**
   * Creates a single directory.
   *
   * @param f path
   * @return true iff the directory exists, or was created
   */
  private boolean mkdir(Path f) throws IOException {
    try {
      FileStatus fileStatus = getFileStatus(f);
      if (fileStatus.isFile()) {
        throw new FileAlreadyExistsException(String.format("Can't make directory for path '%s' since it is a file.", f));
      }
    } catch (FileNotFoundException e) {
      if (LOG.isDebugEnabled()) LOG.debug("Making dir '" + f + "' in OSS");
      String key = pathToKey(f) + FOLDER_SUFFIX;
      store.storeEmptyFile(key);
    }
    return true;
  }

  /**
   * Proxy stream that reads from the given input stream. Each #seek results in an API call to OSS.
   */
  @VisibleForTesting
  static class OSSFileInputStream extends FSInputStream {

    private final FileSystemStore      store;
    private final String               key;
    private final Optional<Statistics> statistics;

    private InputStream in;
    private long pos = 0;

    public OSSFileInputStream(FileSystemStore store, String key, Optional<Statistics> statistics) throws IOException {
      this.store = store;
      this.key = key;
      this.statistics = statistics;
      this.in = store.retrieve(key);
    }

    @Override
    public synchronized int read() throws IOException {
      int result;

      try {
        result = in.read();
      } catch (IOException e) {
        LOG.info("Received IOException while reading '{}', attempting to reopen", key);
        LOG.debug("{}", e, e);
        try {
          seek(pos);
          result = in.read();
        } catch (EOFException eof) {
          LOG.debug("EOF on input stream read: {}", eof, eof);
          result = -1;
        }
      }

      if (-1 != result) {
        pos++;
        if (statistics.isPresent()) statistics.get().incrementBytesRead(1);
      }

      return result;
    }

    @Override
    public synchronized int read(byte[] b, int off, int len) throws IOException {
      if (null == in) throw new EOFException("Cannot read closed stream");

      int result;

      try {
        result = in.read(b, off, len);
      } catch (EOFException eof) {
        throw eof;
      } catch (IOException e) {
        LOG.info("Received IOException while reading '{}', attempting to reopen.", key);
        seek(pos);
        result = in.read(b, off, len);
      }

      if (result > 0) {
        pos += result;
        if (statistics.isPresent()) statistics.get().incrementBytesRead(result);
      }

      return result;
    }

    @Override
    public synchronized void close() throws IOException {
      closeInnerStream();
    }

    /**
     * Close the inner stream if not null. Even if an exception
     * is raised during the close, the field is set to null
     *
     * @throws IOException if raised by the close() operation.
     */
    private void closeInnerStream() throws IOException {
      if (null != in) {
        try {
          in.close();
        } finally {
          in = null;
        }
      }
    }

    /**
     * Update inner stream with a new stream and position
     *
     * @param newStream new stream - must not be null
     * @param newPos    new position
     * @throws IOException IO exception on a failure to close the existing
     *                     stream.
     */
    private synchronized void updateInnerStream(InputStream newStream, long newPos) throws IOException {
      closeInnerStream();
      in = newStream;
      this.pos = newPos;
    }

    @Override
    public synchronized void seek(long newPos) throws IOException {
      if (newPos < 0) throw new EOFException("Cannot seek with a negative position: " + newPos);
      if (pos != newPos) {
        // the seek is attempting to move the current position
        LOG.debug("Opening key '{}' for reading at position '{}", key, newPos);
        InputStream newStream = store.retrieve(key, newPos);
        updateInnerStream(newStream, newPos);
      }
    }

    @Override
    public synchronized long getPos() throws IOException {
      return pos;
    }

    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
      return false;
    }
  }

  /**
   * Proxy stream that forwards all #write calls to a FileOutputStream writing to a temporary local file. When the
   * stream is closed, the file is uploaded to OSS.
   */
  private class OSSFileOutputStream extends OutputStream {

    private Configuration           conf;
    private String                  key;
    private File                    backupFile;
    private OutputStream            backupStream;
    private Optional<MessageDigest> digest;
    private boolean                 closed;
    private LocalDirAllocator       lDirAlloc;
    private Optional<Progressable>  progress;

    public OSSFileOutputStream(Configuration conf,
                               String key,
                               Optional<Progressable> progress) throws IOException {
      this.conf = conf;
      this.key = key;
      this.progress = progress;
      this.backupFile = newBackupFile();

      LOG.info("OutputStream for key '{}' writing to temporary file '{}'", key, this.backupFile);

      // write into the temporary file
      try {
        this.digest = of(MessageDigest.getInstance("MD5"));
        this.backupStream = new BufferedOutputStream(new DigestOutputStream(new FileOutputStream(backupFile), this.digest.get()));
      } catch (NoSuchAlgorithmException e) {
        LOG.warn("Cannot load MD5 digest algorithm skipping message integrity check.", e);
        this.digest = absent();
        this.backupStream = new BufferedOutputStream(new FileOutputStream(backupFile));
      }
    }

    /**
     * Creates a temporary file on the local filesystem.
     *
     * @return file
     */
    private File newBackupFile() throws IOException {
      if (null == conf.get(OSS_BUFFER_DIR_PROPERTY))
        throw new IllegalArgumentException(OSS_BUFFER_DIR_PROPERTY + " is required for writing to OSS.");

      if (null == lDirAlloc)
        lDirAlloc = new LocalDirAllocator(OSS_BUFFER_DIR_PROPERTY);

      File result = lDirAlloc.createTmpFileForWrite("output-", LocalDirAllocator.SIZE_UNKNOWN, conf);
      result.deleteOnExit();
      return result;
    }

    @Override
    public void write(int b) throws IOException {
      backupStream.write(b);
      if (progress.isPresent()) progress.get().progress();
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      backupStream.write(b, off, len);
      if (progress.isPresent()) progress.get().progress();
    }

    @Override
    public void flush() throws IOException {
      backupStream.flush();
    }

    @Override
    public synchronized void close() throws IOException {
      if (closed) return;

      backupStream.close();
      LOG.info("OutputStream for key '{}' closed. Now beginning upload", key);

      try {
        byte[] md5Hash = digest.isPresent() ? digest.get().digest() : null;
        store.storeFile(key, backupFile, fromNullable(md5Hash));
      } finally {
        if (!backupFile.delete()) {
          LOG.warn("Could not delete temporary oss file: {}", backupFile);
        }
        super.close();
        closed = true;
      }
      LOG.info("OutputStream for key '{}' upload complete", key);
    }
  }



  @Override
  @Nullable
  public Path getWorkingDirectory() {
    return workingDir;
  }


  @Override
  public void setWorkingDirectory(Path newDir) {
    workingDir = checkNotNull(newDir);
  }


  @Override
  public boolean mkdirs(Path path, FsPermission permission) throws IOException {
    path = checkNotNull(path);
    Path absolutePath = makeAbsolute(path);
    List<Path> paths = new LinkedList<>();

    // obtain a list of parent directories
    while (null != absolutePath) {
      paths.add(0, absolutePath);
      absolutePath = absolutePath.getParent();
    }

    boolean result = true;
    for (Path p : paths) result &= mkdir(p);

    return result;
  }


  @Override
  public long getDefaultBlockSize(Path path) {
    return getConf().getLong(OSS_BLOCK_SIZE_PROPERTY, DEFAULT_BLOCK_SIZE);
  }
}