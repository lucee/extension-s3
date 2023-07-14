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
```

For further implementation details

https://github.com/lucee/extension-s3/blob/master/source/java/src/org/lucee/extension/resource/s3/S3Properties.java
https://github.com/lucee/extension-s3/blob/master/source/java/src/org/lucee/extension/resource/s3/S3ResourceProvider.java#L142

There are some known issues with region support (especially with other providers than AWS

We are tracking those issues in the following Epic https://luceeserver.atlassian.net/browse/LDEV-4636
