# Lucee S3 Extension

[![Java CI](https://github.com/lucee/extension-s3/actions/workflows/main.yml/badge.svg)](https://github.com/lucee/extension-s3/actions/workflows/main.yml)

Issues: https://luceeserver.atlassian.net/issues/?jql=labels%20%3D%20s3

Docs: https://docs.lucee.org/categories/s3.html

V2 is a complete rewrite using AWSLIB, v0.9.4 used jets3t which is longer maintained

Lucee 5.4 and 6.0 bundles the v2 extension, while we have strived to maintain backward 
compatibility with code written against the older version, there maybe some code changes required

The supported syntax for a S3 resource url is as follows

`s3://#ACCESS_KEY_ID#:#SECRET_KEY#@#HOST#/#bucketName#`

The default host is AWS S3 and that can be ommitted if you are using AWS s3

The s3 v2 extension reads defaults from the following Environment Variables / System Properties

```
lucee.s3.secretaccesskey || lucee.s3.secretkey
lucee.s3.accesskeyid || lucee.s3.accesskey
lucee.s3.host || lucee.s3.server
lucee.s3.location || lucee.s3.defaultLocation or lucee.s3.region
lucee.s3.acl || lucee.s3.accesscontrollist
lucee.s3.pool.maxconnections (extension default: 128; AWS SDK default: 50)
lucee.s3.pool.connectiontimeout
lucee.s3.pool.sockettimeout
lucee.s3.pool.connectionmaxidlemillis
lucee.s3.pool.warnutilization
```

Equivalent environment variables: `LUCEE_S3_POOL_MAXCONNECTIONS`, `LUCEE_S3_POOL_CONNECTIONTIMEOUT`, `LUCEE_S3_POOL_SOCKETTIMEOUT`, `LUCEE_S3_POOL_CONNECTIONMAXIDLEMILLIS`, `LUCEE_S3_POOL_WARNUTILIZATION`.

You can also configure the HTTP connection pool in Application.cfc (one shared AWS SDK client per credential set):

```javascript
this.vfs.s3.pool = {
    maxConnections: 200,              // extension default is 128
    connectionTimeout: 10000,
    socketTimeout: 50000,
    connectionMaxIdleMillis: 60000,
    warnUtilization: 0.8              // log WARN at 80% utilization; 0 to disable
};
```

For further implementation details

https://github.com/lucee/extension-s3/blob/master/source/java/src/org/lucee/extension/resource/s3/S3Properties.java
https://github.com/lucee/extension-s3/blob/master/source/java/src/org/lucee/extension/resource/s3/S3ResourceProvider.java#L142

There are some known issues with region support (especially with other providers than AWS

We are tracking those issues in the following Epic https://luceeserver.atlassian.net/browse/LDEV-4636

## Bucket Name Suffix per Region (DR failover)

Cross-region replication (CRR) needs two buckets with different names, because bucket names are global. With a bucket suffix per region your code keeps using **one logical bucket name** and the extension adds the suffix of the effective region in one central place, right before the request is sent. Switching the region (for example to fail over to a replica region) then switches to the replica bucket, without touching the code.

Nothing is configured by default, so nothing changes unless you define a suffix. A [prefix per region](#bucket-name-prefix-per-region) works the same way and can be combined with the suffix.

```javascript
component {
    this.s3.accessKeyId     = "my-access-key";
    this.s3.awsSecretKey    = "my-secret-key";
    this.s3.defaultLocation = "eu-west-1";   // set the region together with the suffix, see below

    // region -> suffix
    this.s3.bucketSuffixes = { "eu-west-1": "", "eu-central-1": "-dr" };

    // optional: buckets that are not replicated and never get a suffix (exact name or prefix*)
    this.s3.bucketSuffixExclude = [ "shared-assets", "tmp-*" ];
}
```

Or with environment variables / system properties:

```bash
LUCEE_S3_REGION=eu-central-1
LUCEE_S3_BUCKETSUFFIXES={"eu-west-1":"","eu-central-1":"-dr"}
LUCEE_S3_BUCKETSUFFIXEXCLUDE=shared-assets,tmp-*
```

With the configuration above, in the region `eu-central-1` the call `S3Write( value="x", bucketName="invoices", objectName="a.txt" )` and `fileWrite( "s3:///invoices/a.txt", "x" )` write to the bucket `invoices-dr`.

**Where it can be configured** (each of the two settings is resolved independently, the most specific wins): environment variable / system property, then `this.s3` / `this.vfs.s3`, then a `this.vfs.s3.<mapping>` entry. A mapping with `bucketSuffixes = {}` has no suffix at all, which is the way to opt a whole mapping out.

```javascript
this.vfs.s3.archive = {
    accessKeyId: "...", awsSecretKey: "...", defaultLocation: "eu-central-1",
    bucketSuffixes: { "eu-central-1": "-archive" }   // overrides this.s3.bucketSuffixes for s3://archive@/...
};
```

**How the suffix is selected**

* This applies to all `s3*()` functions and to the bucket part of `s3://` paths.
* The effective region is the explicit `location`/`region` argument of a function (`S3CreateBucket`, `S3Write`), otherwise the region of the configuration (region of the `s3://` URL, `defaultLocation` of the mapping, `defaultLocation` of `this.s3`, or `LUCEE_S3_REGION`).
* A region without an entry gets no suffix, an entry with an empty string is the same thing.
* A bucket in `bucketSuffixExclude` never gets a suffix. This is the opt-out for buckets that are not replicated. There is no per call argument for this.
* The suffix is added once. Code always uses the logical name, bucket listings (`S3ListBuckets()`, `directoryList( "s3:///" )`) return the logical names and do not show buckets without the suffix of the effective region.
* A suffix can contain lowercase letters, digits, `.` and `-` (maximum 60 characters). An invalid definition throws an error, it is never ignored.

**Things to be aware of**

* Set the region together with the suffix. The suffix is selected with the configured region (or the explicit region argument), the extension does not look up the region of a bucket to select a suffix. Without a configured region, no suffix is added.
* Presigned URLs (`S3GeneratePresignedURL`) generated during a failover point to the suffixed bucket.
* Full bucket names that your application stored (in a database for example) are not rewritten. The `bucketName` in the info structures (`S3GetInfo`, `fileInfo`, ...) is the real bucket name.
* `application action="update" s3={...}` can not carry `bucketSuffixes` and `bucketSuffixExclude`, define them in `Application.cfc` (`this.s3`, `this.vfs.s3`) or with environment variables / system properties.

**AWS checklist for replicated buckets** (see [AWS: Replicating objects](https://docs.aws.amazon.com/AmazonS3/latest/userguide/replication.html))

* Versioning must be enabled on both buckets.
* If writes happen during a failover, you need two-way replication rules (one for each direction) and replica modification sync, so changes flow back after the failover.
* Existing objects are not replicated by a new rule, use S3 Batch Replication for them.
* Replication of delete markers is optional. Permanent deletes of a specific version are never replicated.
* Replication is asynchronous. With S3 Replication Time Control (RTC) 99.99% of the objects are replicated within 15 minutes, so the newest objects can be missing in the other bucket right after a failover.

## Bucket Name Prefix per Region

The prefix is the counterpart of the [bucket name suffix](#bucket-name-suffix-per-region-dr-failover): your code keeps using **one logical bucket name** and the extension adds the prefix of the effective region in the same central place, right before the request is sent. With the prefix `livestock-` for `us-west-2`, the bucket `susi` is the bucket `livestock-susi` on S3 when the effective region is `us-west-2`.

Nothing is configured by default, so nothing changes unless you define a prefix.

```javascript
component {
    this.s3.accessKeyId     = "my-access-key";
    this.s3.awsSecretKey    = "my-secret-key";
    this.s3.defaultLocation = "us-west-2";   // set the region together with the prefix

    // region -> prefix
    this.s3.bucketPrefixes = { "us-west-2": "livestock-" };

    // optional: buckets that never get a prefix (exact name or prefix*)
    this.s3.bucketPrefixExclude = [ "shared-assets", "tmp-*" ];
}
```

Or with environment variables / system properties:

```bash
LUCEE_S3_REGION=us-west-2
LUCEE_S3_BUCKETPREFIXES={"us-west-2":"livestock-"}
LUCEE_S3_BUCKETPREFIXEXCLUDE=shared-assets,tmp-*
```

With the configuration above `S3Write( value="x", bucketName="susi", objectName="a.txt" )` and `fileWrite( "s3:///susi/a.txt", "x" )` write to the bucket `livestock-susi`.

The prefix works exactly like the suffix:

* It is configured at the same places with the same precedence (environment variable / system property, then `this.s3` / `this.vfs.s3`, then `this.vfs.s3.<mapping>.bucketPrefixes`). `bucketPrefixes` and `bucketPrefixExclude` are resolved independently of each other and independently of the suffix settings. A mapping with `bucketPrefixes = {}` has no prefix, a mapping with `bucketPrefixes = {}` **and** `bucketSuffixes = {}` is not touched at all.
* The effective region, the region without an entry (or an entry with an empty string), the bucket listings that show logical names only, the presigned URLs and the `application action="update"` limitation are the same as for the suffix, see above.
* A bucket in `bucketPrefixExclude` never gets a prefix. This list is independent of `bucketSuffixExclude`: a bucket can be excluded from the prefix and still get the suffix and the other way around.
* A prefix can contain lowercase letters, digits, `.` and `-`, has to start with a letter or a digit (maximum 60 characters). An invalid definition throws an error, it is never ignored.
* The prefix is added once, code always uses the logical name.

**Prefix and suffix together:** when a region has both, the bucket on S3 is `prefix + bucket + suffix`. With `bucketPrefixes = { "us-west-2": "livestock-" }` and `bucketSuffixes = { "us-west-2": "-dr" }` the bucket `susi` is `livestock-susi-dr`. Bucket listings only show buckets that have the prefix and the suffix of the effective region (apart from the buckets in the exclude lists), with the logical name.

---

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
