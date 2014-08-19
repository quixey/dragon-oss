# dragon

An implementation of the HDFS API using the Aliyun OSS client.

## Usage

Include the following properties in `core-site.xml`.

```xml
<property>
    <name>fs.oss.impl</name>
    <value>com.quixey.hadoop.fs.oss.OSSFileSystem</value>
</property>
<property>
    <name>fs.oss.accessKeyId</name>
    <value>...</value>
</property>
<property>
    <name>fs.oss.secretAccessKey</name>
    <value>...</value>
</property>
```

### Configuration

Please refer to `OSSFileSystemConfigKeys` for a list of all configuration
properties.

### Class Path

Use `./gradlew copyDeps` to copy dragon's dependencies (jars) to build/libs.
Ensure that each jar exists on the classpath of whatever Hadoop tool you are
using. For example,

```
$> ./gradlew copyDeps
$> sudo mv libs/dragon-0.1.0.jar /usr/lib/hadoop-hdfs/lib
$> sudo mv libs/aliyun-openservices-OTS-2.0.4.jar /usr/lib/hadoop-hdfs/lib
$> sudo mv libs/gson-2.2.4.jar /usr/lib/hadoop-hdfs/lib
$> sudo mv libs/jdom-1.1.jar /usr/lib/hadoop-hdfs/lib
$> hdfs dfs -ls oss://xyz/
```

## Testing

Some of the tests in the test suite require an OSS connection. If
`core-site.xml` exists on the classpath and contains OSS credentials, the test
suite will attempt a connection to run the OSS tests. Otherwise, these OSS
tests will be skipped.

```bash
$ cp src/test/resources/core-site.xml{.example,}
# fill in core-site.xml with your OSS credentials
$ ./gradlew check
```

## Developing

1. Create a new branch, preferably named after the ticket you are working on.
1. If your work doesn't directly involve the OSS API, temporarily disable the OSS tests by removing your credentials
   from `core-site.xml`. Use the `InMemoryFileSystemStore` if necessary.
1. Write tests, write code, and run them.
1. Use `./gradlew check` when you are done to run FindBugs, PMD, and TestNG checks.
