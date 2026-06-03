# Lucee S3 Extension

[![Java CI](https://github.com/lucee/extension-s3/actions/workflows/main.yml/badge.svg)](https://github.com/lucee/extension-s3/actions/workflows/main.yml)

Adds S3-compatible object storage support to Lucee via a Virtual File System (`s3://` paths) and a set of native `s3*()` functions. Works with AWS S3 and any S3-compatible provider including MinIO, Backblaze B2, Wasabi, Google Cloud Storage, and DigitalOcean Spaces.

- **Issues:** https://luceeserver.atlassian.net/issues/?jql=labels%20%3D%20s3
- **Docs:** https://docs.lucee.org/categories/s3.html

---

## Installation

Install via the Lucee Administrator under **Extension > Applications**, or declare it in `.CFConfig.json`:

```json
{
  "extensions": [
    {
      "id": "17AB52DE-B300-A94B-E058BD978511E39E",
      "name": "S3 Extension"
    }
  ]
}
```

Lucee 5.4 and 6.0 bundle v2 of the extension. v2 is a complete rewrite using the AWS SDK; older versions used jets3t which is no longer maintained. While backward compatibility has been maintained where possible, some code changes may be required when upgrading from v0.x.

---

## Configuration

Settings can be provided in three ways, applied in this order of precedence: **inline URL** > **Application.cfc** > **environment variables / system properties**.

### Application.cfc

```javascript
component {
    this.name = "myapp";

    this.vfs.s3.accessKeyId     = "my-access-key";
    this.vfs.s3.awsSecretKey    = "my-secret-key";
    this.vfs.s3.host            = "s3.amazonaws.com";   // omit for AWS default
    this.vfs.s3.defaultLocation = "us-east-1";

    // optional
    this.vfs.s3.pathStyleAccess = true;   // force path-style URLs (auto-detected for unknown hosts)
    this.vfs.s3.ssl             = true;   // false = plain HTTP (e.g. local MinIO without TLS)
    this.vfs.s3.acl             = "private";

    // HTTP connection pool (AWS SDK, per shared client / credential set)
    this.vfs.s3.pool = {
        maxConnections: 200,              // extension default is 128 (AWS SDK default is 50)
        connectionTimeout: 10000,         // ms to wait for a pool slot (default 10000)
        socketTimeout: 50000,             // ms read timeout on an active connection
        connectionMaxIdleMillis: 60000,   // discard idle pooled connections
        warnUtilization: 0.8              // log WARN at 80% utilization; 0 to disable
    };
}
```

### Environment Variables / System Properties

| Environment Variable          | System Property                  | Description                                      |
|-------------------------------|----------------------------------|--------------------------------------------------|
| `LUCEE_S3_ACCESSKEYID`        | `lucee.s3.accesskeyid`           | Access key ID                                    |
| `LUCEE_S3_SECRETACCESSKEY`    | `lucee.s3.secretaccesskey`       | Secret access key                                |
| `LUCEE_S3_HOST`               | `lucee.s3.host`                  | Custom endpoint host, e.g. `minio:9000`          |
| `LUCEE_S3_REGION`             | `lucee.s3.region`                | Region / location, e.g. `us-east-1`              |
| `LUCEE_S3_ACL`                | `lucee.s3.acl`                   | Default ACL, e.g. `public-read`                  |
| `LUCEE_S3_CACHEREGION`        | `lucee.s3.cacheregion`           | Cache bucket region lookups (`true`/`false`)     |
| `LUCEE_S3_PATHSTYLEACCESS`    | `lucee.s3.pathstyleaccess`       | Force path-style URLs (`true`/`false`)           |
| `LUCEE_S3_SSL`                | `lucee.s3.ssl`                   | Use HTTPS (`true`, default) or plain HTTP (`false`) |
| `LUCEE_S3_POOL_MAXCONNECTIONS` | `lucee.s3.pool.maxconnections`  | Max concurrent HTTP connections per S3 client (extension default: 128; AWS SDK default: 50) |
| `LUCEE_S3_POOL_CONNECTIONTIMEOUT` | `lucee.s3.pool.connectiontimeout` | Ms to wait for a connection from the pool (default: 10000) |
| `LUCEE_S3_POOL_SOCKETTIMEOUT` | `lucee.s3.pool.sockettimeout`    | Socket read timeout in ms (default: 50000) |
| `LUCEE_S3_POOL_CONNECTIONMAXIDLEMILLIS` | `lucee.s3.pool.connectionmaxidlemillis` | Idle connection TTL in ms (default: 60000) |
| `LUCEE_S3_POOL_WARNUTILIZATION` | `lucee.s3.pool.warnutilization` | Log when in-flight requests reach this fraction of `maxConnections` (default: 0.8; use `0` to disable) |

Pool messages are written to the S3 log channel (`s3` or `application`). When the pool is exhausted you get an ERROR with `Timeout waiting for connection from pool`; high utilization logs WARN at most once per minute.

Region is derived from a cached bucket lookup, an explicit `defaultLocation`, or from the endpoint host for **custom** providers (e.g. `s3.eu-central-1.wasabisys.com` → `eu-central-1`). AWS uses `getBucketLocation` per bucket when no cached region exists. Expired SDK clients are shut down when replaced so their HTTP connections are released back to the OS.

### Inline URL Credentials

Credentials can be embedded directly in the path:

```
s3://{accessKeyId}:{secretKey}@{host}/{bucket}/path/to/file.txt
s3://{accessKeyId}:{secretKey}:{region}@/{bucket}/path/to/file.txt
```

The host can be omitted for AWS S3:

```
s3://{accessKeyId}:{secretKey}@/{bucket}/path/to/file.txt
```

---

## Usage

### Virtual File System

With credentials configured, all standard Lucee file functions work against S3 using `s3:///` paths (three slashes — host is omitted, third slash starts the bucket):

```javascript
fileWrite("s3:///my-bucket/hello.txt", "Hello World");

content = fileRead("s3:///my-bucket/hello.txt");

exists = fileExists("s3:///my-bucket/hello.txt");

files = directoryList("s3:///my-bucket/", false, "query");

fileCopy("s3:///my-bucket/hello.txt", "s3:///other-bucket/hello.txt");

fileDelete("s3:///my-bucket/hello.txt");
```

### Native S3 Functions

The extension also provides native `s3*()` functions for S3-specific operations:

```javascript
// buckets
buckets = s3listBuckets();
objects = s3listBucket("my-bucket");

// read / write
content = s3read("my-bucket", "hello.txt");
binary  = s3readBinary("my-bucket", "image.png");
s3write("my-bucket", "hello.txt", "Hello World");

// copy / move / delete
s3copy("my-bucket", "hello.txt", "other-bucket", "copy.txt");
s3move("my-bucket", "hello.txt", "other-bucket", "moved.txt");
s3delete("my-bucket", "hello.txt");

// metadata / ACL
meta = s3getMetaData("my-bucket", "hello.txt");
s3setMetaData("my-bucket", "hello.txt", { "x-custom-tag": "value" });
acl  = s3getACL("my-bucket", "hello.txt");

// URLs
url = s3generatePresignedUrl("my-bucket", "hello.txt", 3600);
uri = s3generateUri("my-bucket", "hello.txt");
```

---

## Self-Hosted Providers (MinIO, LocalStack)

Self-hosted S3-compatible providers typically need two additional settings:

- **`pathStyleAccess = true`** — forces the SDK to use `host/bucket` URLs instead of `bucket.host`, which fails DNS resolution for non-AWS hostnames. Lucee automatically enables this for unrecognised hostnames, but you can set it explicitly to guarantee the behaviour.
- **`ssl = false`** — disables HTTPS for providers running over plain HTTP.

```javascript
this.vfs.s3.accessKeyId     = "minioadmin";
this.vfs.s3.awsSecretKey    = "minioadmin";
this.vfs.s3.host            = "minio:9000";
this.vfs.s3.defaultLocation = "us-east-1";
this.vfs.s3.pathStyleAccess = true;
this.vfs.s3.ssl             = false;
```

Equivalent via environment variables:

```
LUCEE_S3_ACCESSKEYID=minioadmin
LUCEE_S3_SECRETACCESSKEY=minioadmin
LUCEE_S3_HOST=minio:9000
LUCEE_S3_REGION=us-east-1
LUCEE_S3_PATHSTYLEACCESS=true
LUCEE_S3_SSL=false
```

---

## Known Issues

Region support with non-AWS providers has some rough edges. Ongoing work is tracked in:
https://luceeserver.atlassian.net/browse/LDEV-4636
