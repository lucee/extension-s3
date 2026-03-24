package org.lucee.extension.resource.s3;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import lucee.commons.io.log.Log;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.model.S3Exception;

public class AmazonS3Client implements S3Client {

	private static Map<String, AmazonS3Client> pool = new ConcurrentHashMap<String, AmazonS3Client>();

	// private S3 s3;
	private org.lucee.extension.resource.s3.region.RegionFactory.Region region;

	private S3Client client;
	private Log log;

	private long created;

	private String accessKeyId;
	private String secretAccessKey;
	private String host;

	private long liveTimeout;

	private boolean pathStyleAccess;

	public static AmazonS3Client get(String accessKeyId, String secretAccessKey, String host, org.lucee.extension.resource.s3.region.RegionFactory.Region region, long liveTimeout,
			boolean pathStyleAccess, Log log) throws S3Exception {

		String key = accessKeyId + ":" + secretAccessKey + ":" + host + ":" + (region == null ? "default-region" : S3.toString(region)) + ":" + pathStyleAccess;
		AmazonS3Client client = pool.get(key);
		if (client == null || client.isExpired()) {
			synchronized (pool) {
				client = pool.get(key);
				if (client == null || client.isExpired()) {
					pool.put(key, client = new AmazonS3Client(accessKeyId, secretAccessKey, host, region, key, liveTimeout, pathStyleAccess, log));
					if (log != null) log.debug("S3", "create client for  [" + accessKeyId + ":...@" + host + "]");
				}
			}

		}
		return client;
	}

	private AmazonS3Client(String accessKeyId, String secretAccessKey, String host, org.lucee.extension.resource.s3.region.RegionFactory.Region region, String key,
			long liveTimeout, boolean pathStyleAccess, Log log) throws S3Exception {
		this.accessKeyId = accessKeyId;
		this.secretAccessKey = secretAccessKey;
		this.host = host;
		this.region = region;
		this.pathStyleAccess = pathStyleAccess;
		this.log = log;
		this.created = System.currentTimeMillis();
		client = create();
		this.liveTimeout = liveTimeout;
	}

	public S3Client create() throws S3Exception {
		S3ClientBuilder builder = S3Client.builder().credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKeyId, secretAccessKey)));

		// region or endpoint and region
		if (host != null && !host.isEmpty() && !host.equalsIgnoreCase(S3.DEFAULT_HOST)) {
			String resolvedRegion = region == null ? "us-east-1" : S3.toString(region);
			builder = builder.endpointOverride(URI.create(host.startsWith("http") ? host : "https://" + host)).region(Region.of(resolvedRegion));
		}
		else {
			if (region != null) {
				builder = builder.region(Region.of(region.getName()));
			}
			else {
				builder = builder.region(Region.US_EAST_1).crossRegionAccessEnabled(true);
			}
		}

		if (pathStyleAccess) {
			builder = builder.forcePathStyle(pathStyleAccess);
		}

		return builder.build();
	}

	public boolean isBackBlaze() {
		return host != null && host.toLowerCase().indexOf(S3.BACKBLAZE.toLowerCase()) != -1;
	}

	public boolean isWasabi() {
		return host != null && host.toLowerCase().indexOf(S3.WASABI.toLowerCase()) != -1;
	}

	public boolean isGoogle() {
		return host != null && host.toLowerCase().indexOf(S3.GOOGLE.toLowerCase()) != -1;
	}

	private boolean isExpired() {
		return (liveTimeout + System.currentTimeMillis()) < created;
	}

	@Override
	public AbortMultipartUploadResponse abortMultipartUpload(AbortMultipartUploadRequest amur) throws AwsServiceException, SdkClientException, S3Exception {
		try {
			return client.abortMultipartUpload(amur);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.abortMultipartUpload(amur);
		}
	}

	@Override
	public CompleteMultipartUploadResponse completeMultipartUpload(CompleteMultipartUploadRequest arg0) throws AwsServiceException, SdkClientException, S3Exception {
		try {
			return client.completeMultipartUpload(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.completeMultipartUpload(arg0);
		}
	}

	@Override
	public CopyObjectResponse copyObject(CopyObjectRequest arg0) throws AwsServiceException, SdkClientException, S3Exception {
		try {
			return client.copyObject(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.copyObject(arg0);
		}
	}

	@Override
	public CreateBucketResponse createBucket(CreateBucketRequest arg0) throws AwsServiceException, SdkClientException, S3Exception {
		try {
			return client.createBucket(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.createBucket(arg0);
		}
	}

	@Override
	public DeleteBucketResponse deleteBucket(DeleteBucketRequest arg0) throws AwsServiceException, SdkClientException, S3Exception {
		try {
			return client.deleteBucket(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.deleteBucket(arg0);
		}
	}

	@Override
	public DeleteBucketAnalyticsConfigurationResponse deleteBucketAnalyticsConfiguration(DeleteBucketAnalyticsConfigurationRequest arg0)
			throws AwsServiceException, SdkClientException, S3Exception {
		try {
			return client.deleteBucketAnalyticsConfiguration(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.deleteBucketAnalyticsConfiguration(arg0);
		}
	}

	@Override
	public DeleteBucketEncryptionResponse deleteBucketEncryption(DeleteBucketEncryptionRequest arg0) throws AwsServiceException, SdkClientException, S3Exception {
		try {
			return client.deleteBucketEncryption(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.deleteBucketEncryption(arg0);
		}
	}

	@Override
	public DeleteBucketIntelligentTieringConfigurationResponse deleteBucketIntelligentTieringConfiguration(DeleteBucketIntelligentTieringConfigurationRequest arg0)
			throws AwsServiceException, SdkClientException, S3Exception {
		try {
			return client.deleteBucketIntelligentTieringConfiguration(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.deleteBucketIntelligentTieringConfiguration(arg0);
		}
	}

	@Override
	public DeleteBucketInventoryConfigurationResponse deleteBucketInventoryConfiguration(DeleteBucketInventoryConfigurationRequest arg0)
			throws AwsServiceException, SdkClientException, S3Exception {
		try {
			return client.deleteBucketInventoryConfiguration(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.deleteBucketInventoryConfiguration(arg0);
		}
	}

	@Override
	public DeleteBucketMetricsConfigurationResponse deleteBucketMetricsConfiguration(DeleteBucketMetricsConfigurationRequest arg0)
			throws AwsServiceException, SdkClientException, S3Exception {
		try {
			return client.deleteBucketMetricsConfiguration(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.deleteBucketMetricsConfiguration(arg0);
		}
	}

	@Override
	public DeleteBucketOwnershipControlsResponse deleteBucketOwnershipControls(DeleteBucketOwnershipControlsRequest arg0)
			throws AwsServiceException, SdkClientException, S3Exception {
		try {
			return client.deleteBucketOwnershipControls(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.deleteBucketOwnershipControls(arg0);
		}
	}

	@Override
	public DeleteBucketPolicyResponse deleteBucketPolicy(DeleteBucketPolicyRequest arg0) throws AwsServiceException, SdkClientException, S3Exception {
		try {
			return client.deleteBucketPolicy(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.deleteBucketPolicy(arg0);
		}
	}

	@Override
	public DeleteObjectResponse deleteObject(DeleteObjectRequest arg0) throws AwsServiceException, SdkClientException, S3Exception {
		try {
			return client.deleteObject(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.deleteObject(arg0);
		}
	}

	@Override
	public DeleteObjectTaggingResponse deleteObjectTagging(DeleteObjectTaggingRequest arg0) throws AwsServiceException, SdkClientException, S3Exception {
		try {
			return client.deleteObjectTagging(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.deleteObjectTagging(arg0);
		}
	}

	@Override
	public DeleteObjectsResponse deleteObjects(DeleteObjectsRequest arg0) throws AwsServiceException, SdkClientException, S3Exception {
		try {
			return client.deleteObjects(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.deleteObjects(arg0);
		}
	}

	@Override
	public DeletePublicAccessBlockResponse deletePublicAccessBlock(DeletePublicAccessBlockRequest arg0) throws AwsServiceException, SdkClientException, S3Exception {
		try {
			return client.deletePublicAccessBlock(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.deletePublicAccessBlock(arg0);
		}
	}

	@Override
	public GetBucketAccelerateConfigurationResponse getBucketAccelerateConfiguration(GetBucketAccelerateConfigurationRequest arg0)
			throws AwsServiceException, SdkClientException, S3Exception {
		try {
			return client.getBucketAccelerateConfiguration(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.getBucketAccelerateConfiguration(arg0);
		}
	}

	@Override
	public GetBucketAclResponse getBucketAcl(GetBucketAclRequest arg0) throws AwsServiceException, SdkClientException, S3Exception {
		try {
			return client.getBucketAcl(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.getBucketAcl(arg0);
		}
	}

	@Override
	public GetBucketAnalyticsConfigurationResponse getBucketAnalyticsConfiguration(GetBucketAnalyticsConfigurationRequest arg0)
			throws AwsServiceException, SdkClientException, S3Exception {
		try {
			return client.getBucketAnalyticsConfiguration(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.getBucketAnalyticsConfiguration(arg0);
		}
	}

	@Override
	public GetBucketEncryptionResponse getBucketEncryption(GetBucketEncryptionRequest arg0) throws AwsServiceException, SdkClientException, S3Exception {
		try {
			return client.getBucketEncryption(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.getBucketEncryption(arg0);
		}
	}

	@Override
	public GetBucketIntelligentTieringConfigurationResponse getBucketIntelligentTieringConfiguration(GetBucketIntelligentTieringConfigurationRequest arg0)
			throws AwsServiceException, SdkClientException, S3Exception {
		try {
			return client.getBucketIntelligentTieringConfiguration(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.getBucketIntelligentTieringConfiguration(arg0);
		}
	}

	@Override
	public GetBucketInventoryConfigurationResponse getBucketInventoryConfiguration(GetBucketInventoryConfigurationRequest arg0)
			throws AwsServiceException, SdkClientException, S3Exception {
		try {
			return client.getBucketInventoryConfiguration(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.getBucketInventoryConfiguration(arg0);
		}
	}

	@Override
	public GetBucketLifecycleConfigurationResponse getBucketLifecycleConfiguration(GetBucketLifecycleConfigurationRequest arg0)
			throws AwsServiceException, SdkClientException, S3Exception {
		try {
			return client.getBucketLifecycleConfiguration(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.getBucketLifecycleConfiguration(arg0);
		}
	}

	@Override
	public GetBucketLocationResponse getBucketLocation(GetBucketLocationRequest arg0) throws AwsServiceException, SdkClientException, S3Exception {
		try {
			return client.getBucketLocation(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.getBucketLocation(arg0);
		}
	}

	@Override
	public GetBucketMetricsConfigurationResponse getBucketMetricsConfiguration(GetBucketMetricsConfigurationRequest arg0)
			throws AwsServiceException, SdkClientException, S3Exception {
		try {
			return client.getBucketMetricsConfiguration(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.getBucketMetricsConfiguration(arg0);
		}
	}

	@Override
	public GetBucketNotificationConfigurationResponse getBucketNotificationConfiguration(GetBucketNotificationConfigurationRequest arg0)
			throws AwsServiceException, SdkClientException, S3Exception {
		try {
			return client.getBucketNotificationConfiguration(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.getBucketNotificationConfiguration(arg0);
		}
	}

	@Override
	public GetBucketOwnershipControlsResponse getBucketOwnershipControls(GetBucketOwnershipControlsRequest arg0) throws AwsServiceException, SdkClientException, S3Exception {
		try {
			return client.getBucketOwnershipControls(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.getBucketOwnershipControls(arg0);
		}
	}

	@Override
	public GetBucketPolicyResponse getBucketPolicy(GetBucketPolicyRequest arg0) throws AwsServiceException, SdkClientException, S3Exception {
		try {
			return client.getBucketPolicy(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.getBucketPolicy(arg0);
		}
	}

	@Override
	public GetBucketPolicyStatusResponse getBucketPolicyStatus(GetBucketPolicyStatusRequest arg0) throws AwsServiceException, SdkClientException, S3Exception {
		try {
			return client.getBucketPolicyStatus(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.getBucketPolicyStatus(arg0);
		}
	}

	@Override
	public ResponseInputStream<GetObjectResponse> getObject(GetObjectRequest arg0) throws AwsServiceException, SdkClientException, S3Exception {
		try {
			return client.getObject(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.getObject(arg0);
		}
	}

	@Override
	public GetObjectAclResponse getObjectAcl(GetObjectAclRequest arg0) throws AwsServiceException, SdkClientException, S3Exception {
		try {
			return client.getObjectAcl(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.getObjectAcl(arg0);
		}
	}

	@Override
	public GetObjectLegalHoldResponse getObjectLegalHold(GetObjectLegalHoldRequest arg0) throws AwsServiceException, SdkClientException, S3Exception {
		try {
			return client.getObjectLegalHold(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.getObjectLegalHold(arg0);
		}
	}

	@Override
	public GetObjectLockConfigurationResponse getObjectLockConfiguration(GetObjectLockConfigurationRequest arg0) throws AwsServiceException, SdkClientException, S3Exception {
		try {
			return client.getObjectLockConfiguration(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.getObjectLockConfiguration(arg0);
		}
	}

	@Override
	public GetObjectRetentionResponse getObjectRetention(GetObjectRetentionRequest arg0) throws AwsServiceException, SdkClientException, S3Exception {
		try {
			return client.getObjectRetention(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.getObjectRetention(arg0);
		}
	}

	@Override
	public GetObjectTaggingResponse getObjectTagging(GetObjectTaggingRequest arg0) throws AwsServiceException, SdkClientException, S3Exception {
		try {
			return client.getObjectTagging(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.getObjectTagging(arg0);
		}
	}

	@Override
	public GetPublicAccessBlockResponse getPublicAccessBlock(GetPublicAccessBlockRequest arg0) throws AwsServiceException, SdkClientException, S3Exception {
		try {
			return client.getPublicAccessBlock(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.getPublicAccessBlock(arg0);
		}
	}

	@Override
	public HeadBucketResponse headBucket(HeadBucketRequest arg0) throws AwsServiceException, SdkClientException, S3Exception {
		try {
			return client.headBucket(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.headBucket(arg0);
		}
	}

	@Override
	public ListBucketAnalyticsConfigurationsResponse listBucketAnalyticsConfigurations(ListBucketAnalyticsConfigurationsRequest arg0)
			throws AwsServiceException, SdkClientException, S3Exception {
		try {
			return client.listBucketAnalyticsConfigurations(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.listBucketAnalyticsConfigurations(arg0);
		}
	}

	@Override
	public ListBucketIntelligentTieringConfigurationsResponse listBucketIntelligentTieringConfigurations(ListBucketIntelligentTieringConfigurationsRequest arg0)
			throws AwsServiceException, SdkClientException, S3Exception {
		try {
			return client.listBucketIntelligentTieringConfigurations(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.listBucketIntelligentTieringConfigurations(arg0);
		}
	}

	@Override
	public ListBucketInventoryConfigurationsResponse listBucketInventoryConfigurations(ListBucketInventoryConfigurationsRequest arg0)
			throws AwsServiceException, SdkClientException, S3Exception {
		try {
			return client.listBucketInventoryConfigurations(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.listBucketInventoryConfigurations(arg0);
		}
	}

	@Override
	public ListBucketMetricsConfigurationsResponse listBucketMetricsConfigurations(ListBucketMetricsConfigurationsRequest arg0)
			throws AwsServiceException, SdkClientException, S3Exception {
		try {
			return client.listBucketMetricsConfigurations(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.listBucketMetricsConfigurations(arg0);
		}
	}

	@Override
	public ListBucketsResponse listBuckets() throws AwsServiceException, SdkClientException, S3Exception {
		try {
			return client.listBuckets();
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.listBuckets();
		}
	}

	@Override
	public ListBucketsResponse listBuckets(ListBucketsRequest arg0) throws AwsServiceException, SdkClientException, S3Exception {
		try {
			return client.listBuckets(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.listBuckets(arg0);
		}
	}

	@Override
	public ListMultipartUploadsResponse listMultipartUploads(ListMultipartUploadsRequest arg0) throws AwsServiceException, SdkClientException, S3Exception {
		try {
			return client.listMultipartUploads(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.listMultipartUploads(arg0);
		}
	}

	@Override
	public ListObjectsResponse listObjects(ListObjectsRequest arg0) throws AwsServiceException, SdkClientException, S3Exception {
		try {
			return client.listObjects(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.listObjects(arg0);
		}
	}

	@Override
	public ListObjectsV2Response listObjectsV2(ListObjectsV2Request arg0) throws AwsServiceException, SdkClientException, S3Exception {
		try {
			return client.listObjectsV2(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.listObjectsV2(arg0);
		}
	}

	@Override
	public ListPartsResponse listParts(ListPartsRequest arg0) throws AwsServiceException, SdkClientException, S3Exception {
		try {
			return client.listParts(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.listParts(arg0);
		}
	}

	@Override
	public PutObjectResponse putObject(PutObjectRequest putObjectRequest, RequestBody requestBody) throws AwsServiceException, SdkClientException, S3Exception {
		try {
			return client.putObject(putObjectRequest, requestBody);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.putObject(putObjectRequest, requestBody);
		}
	}

	@Override
	public RestoreObjectResponse restoreObject(RestoreObjectRequest arg0) throws ObjectAlreadyInActiveTierErrorException, AwsServiceException, SdkClientException, S3Exception {
		try {
			return client.restoreObject(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.restoreObject(arg0);
		}
	}

	@Override
	public UploadPartResponse uploadPart(UploadPartRequest uploadPartRequest, RequestBody requestBody) throws AwsServiceException, SdkClientException, S3Exception {
		try {
			return client.uploadPart(uploadPartRequest, requestBody);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.uploadPart(uploadPartRequest, requestBody);
		}
	}

	@Override
	public WriteGetObjectResponseResponse writeGetObjectResponse(WriteGetObjectResponseRequest writeGetObjectResponseRequest, RequestBody requestBody)
			throws AwsServiceException, SdkClientException, S3Exception {
		try {
			return client.writeGetObjectResponse(writeGetObjectResponseRequest, requestBody);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.writeGetObjectResponse(writeGetObjectResponseRequest, requestBody);
		}
	}

	@Override
	public String serviceName() {
		try {
			return client.serviceName();
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.serviceName();
		}
	}

	@Override
	public void close() {
		try {
			client.close();
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			client.close();
		}
	}

	private void invalidateAmazonS3(IllegalStateException ise) throws S3Exception {
		if (log != null) log.log(Log.LEVEL_WARN, "S3", ise);
		try {
			client = create();
		}
		catch (Exception e) {
			if (log != null) log.error("S3", e);
			throw S3Exception.builder().message("failed to invalidate client").cause(e).build();
		}
	}

	public void release() throws S3Exception {
		// FUTURE remove method
	}

}
