package org.lucee.extension.resource.s3;

import java.io.File;
import java.io.InputStream;
import java.net.URL;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.lucee.extension.resource.s3.region.RegionFactory;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.HttpMethod;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.S3ResponseMetadata;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.s3.model.analytics.AnalyticsConfiguration;
import com.amazonaws.services.s3.model.intelligenttiering.IntelligentTieringConfiguration;
import com.amazonaws.services.s3.model.inventory.InventoryConfiguration;
import com.amazonaws.services.s3.model.metrics.MetricsConfiguration;
import com.amazonaws.services.s3.model.ownership.OwnershipControls;
import com.amazonaws.services.s3.waiters.AmazonS3Waiters;

import lucee.commons.io.log.Log;

public class AmazonS3Client implements AmazonS3 {

	private static Map<String, AmazonS3Client> pool = new ConcurrentHashMap<String, AmazonS3Client>();

	// private S3 s3;
	private org.lucee.extension.resource.s3.region.RegionFactory.Region region;

	private AmazonS3 client;
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
			pool.put(key, client = new AmazonS3Client(accessKeyId, secretAccessKey, host, region, key, liveTimeout, pathStyleAccess, log));
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

	public AmazonS3 create() throws S3Exception {
		AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard();
		builder.withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKeyId, secretAccessKey)));
		// region or endpoint and region
		if (host != null && !host.isEmpty() && !host.equalsIgnoreCase(S3.DEFAULT_HOST)) {
			// TODO serviceEndpoint - the service endpoint either with or without the protocol (e.g.
			// https://sns.us-west-1.amazonaws.com or sns.us-west-1.amazonaws.com)

			builder = builder.withEndpointConfiguration(new EndpointConfiguration(host, region == null ? "us-east-1" : S3.toString(region)));
		}
		else {
			if (region != null) {
				builder = builder.withRegion(region.getName());
			}
			else {
				builder = builder.withRegion(RegionFactory.US_EAST_1.getName()).withForceGlobalBucketAccessEnabled(true); // The first region to try your request against
				// If a bucket is in a different region, try again in the correct region

			}
		}
		if (pathStyleAccess) builder.withPathStyleAccessEnabled(pathStyleAccess);

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
	public void abortMultipartUpload(AbortMultipartUploadRequest amur) throws SdkClientException, AmazonServiceException {
		try {
			client.abortMultipartUpload(amur);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			client.abortMultipartUpload(amur);
		}
	}

	@Override
	public void changeObjectStorageClass(String arg0, String arg1, StorageClass arg2) throws SdkClientException, AmazonServiceException {
		try {
			client.changeObjectStorageClass(arg0, arg1, arg2);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			client.changeObjectStorageClass(arg0, arg1, arg2);
		}
	}

	@Override
	public CompleteMultipartUploadResult completeMultipartUpload(CompleteMultipartUploadRequest arg0) throws SdkClientException, AmazonServiceException {
		try {
			return client.completeMultipartUpload(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.completeMultipartUpload(arg0);
		}
	}

	@Override
	public CopyObjectResult copyObject(CopyObjectRequest arg0) throws SdkClientException, AmazonServiceException {
		try {
			return client.copyObject(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.copyObject(arg0);
		}
	}

	@Override
	public CopyObjectResult copyObject(String arg0, String arg1, String arg2, String arg3) throws SdkClientException, AmazonServiceException {
		try {
			return client.copyObject(arg0, arg1, arg2, arg3);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.copyObject(arg0, arg1, arg2, arg3);
		}
	}

	@Override
	public CopyPartResult copyPart(CopyPartRequest arg0) throws SdkClientException, AmazonServiceException {
		try {
			return client.copyPart(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.copyPart(arg0);
		}
	}

	@Override
	public Bucket createBucket(CreateBucketRequest arg0) throws SdkClientException, AmazonServiceException {
		try {
			return client.createBucket(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.createBucket(arg0);
		}
	}

	@Override
	public Bucket createBucket(String arg0) throws SdkClientException, AmazonServiceException {
		try {
			return client.createBucket(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.createBucket(arg0);
		}
	}

	@Override
	public Bucket createBucket(String arg0, Region arg1) throws SdkClientException, AmazonServiceException {
		try {
			return client.createBucket(arg0, arg1);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.createBucket(arg0, arg1);
		}
	}

	@Override
	public Bucket createBucket(String arg0, String arg1) throws SdkClientException, AmazonServiceException {
		try {
			return client.createBucket(arg0, arg1);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.createBucket(arg0, arg1);
		}
	}

	@Override
	public void deleteBucket(DeleteBucketRequest arg0) throws SdkClientException, AmazonServiceException {
		try {
			client.deleteBucket(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			client.deleteBucket(arg0);
		}
	}

	@Override
	public void deleteBucket(String bucketName) throws SdkClientException, AmazonServiceException {
		DeleteBucketRequest dbr = new DeleteBucketRequest(bucketName);

		try {
			client.deleteBucket(dbr);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			client.deleteBucket(dbr);
		}
	}

	@Override
	public DeleteBucketAnalyticsConfigurationResult deleteBucketAnalyticsConfiguration(DeleteBucketAnalyticsConfigurationRequest arg0)
			throws AmazonServiceException, SdkClientException {
		try {
			return client.deleteBucketAnalyticsConfiguration(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.deleteBucketAnalyticsConfiguration(arg0);
		}
	}

	@Override
	public DeleteBucketAnalyticsConfigurationResult deleteBucketAnalyticsConfiguration(String arg0, String arg1) throws AmazonServiceException, SdkClientException {
		try {
			return client.deleteBucketAnalyticsConfiguration(arg0, arg1);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.deleteBucketAnalyticsConfiguration(arg0, arg1);
		}
	}

	@Override
	public void deleteBucketCrossOriginConfiguration(String arg0) {
		try {
			client.deleteBucketCrossOriginConfiguration(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			client.deleteBucketCrossOriginConfiguration(arg0);
		}
	}

	@Override
	public void deleteBucketCrossOriginConfiguration(DeleteBucketCrossOriginConfigurationRequest arg0) {
		try {
			client.deleteBucketCrossOriginConfiguration(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			client.deleteBucketCrossOriginConfiguration(arg0);
		}
	}

	@Override
	public DeleteBucketEncryptionResult deleteBucketEncryption(String arg0) throws AmazonServiceException, SdkClientException {
		try {
			return client.deleteBucketEncryption(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.deleteBucketEncryption(arg0);
		}
	}

	@Override
	public DeleteBucketEncryptionResult deleteBucketEncryption(DeleteBucketEncryptionRequest arg0) throws AmazonServiceException, SdkClientException {
		try {
			return client.deleteBucketEncryption(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.deleteBucketEncryption(arg0);
		}
	}

	@Override
	public DeleteBucketIntelligentTieringConfigurationResult deleteBucketIntelligentTieringConfiguration(DeleteBucketIntelligentTieringConfigurationRequest arg0)
			throws AmazonServiceException, SdkClientException {
		try {
			return client.deleteBucketIntelligentTieringConfiguration(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.deleteBucketIntelligentTieringConfiguration(arg0);
		}
	}

	@Override
	public DeleteBucketIntelligentTieringConfigurationResult deleteBucketIntelligentTieringConfiguration(String arg0, String arg1)
			throws AmazonServiceException, SdkClientException {
		try {
			return client.deleteBucketIntelligentTieringConfiguration(arg0, arg1);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.deleteBucketIntelligentTieringConfiguration(arg0, arg1);
		}
	}

	@Override
	public DeleteBucketInventoryConfigurationResult deleteBucketInventoryConfiguration(DeleteBucketInventoryConfigurationRequest arg0)
			throws AmazonServiceException, SdkClientException {
		try {
			return client.deleteBucketInventoryConfiguration(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.deleteBucketInventoryConfiguration(arg0);
		}
	}

	@Override
	public DeleteBucketInventoryConfigurationResult deleteBucketInventoryConfiguration(String arg0, String arg1) throws AmazonServiceException, SdkClientException {
		try {
			return client.deleteBucketInventoryConfiguration(arg0, arg1);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.deleteBucketInventoryConfiguration(arg0, arg1);
		}
	}

	@Override
	public void deleteBucketLifecycleConfiguration(String arg0) {
		try {
			client.deleteBucketLifecycleConfiguration(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			client.deleteBucketLifecycleConfiguration(arg0);
		}
	}

	@Override
	public void deleteBucketLifecycleConfiguration(DeleteBucketLifecycleConfigurationRequest arg0) {
		try {
			client.deleteBucketLifecycleConfiguration(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			client.deleteBucketLifecycleConfiguration(arg0);
		}
	}

	@Override
	public DeleteBucketMetricsConfigurationResult deleteBucketMetricsConfiguration(DeleteBucketMetricsConfigurationRequest arg0) throws AmazonServiceException, SdkClientException {
		try {
			return client.deleteBucketMetricsConfiguration(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.deleteBucketMetricsConfiguration(arg0);
		}
	}

	@Override
	public DeleteBucketMetricsConfigurationResult deleteBucketMetricsConfiguration(String arg0, String arg1) throws AmazonServiceException, SdkClientException {
		try {
			return client.deleteBucketMetricsConfiguration(arg0, arg1);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.deleteBucketMetricsConfiguration(arg0, arg1);
		}
	}

	@Override
	public DeleteBucketOwnershipControlsResult deleteBucketOwnershipControls(DeleteBucketOwnershipControlsRequest arg0) throws AmazonServiceException, SdkClientException {
		try {
			return client.deleteBucketOwnershipControls(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.deleteBucketOwnershipControls(arg0);
		}
	}

	@Override
	public void deleteBucketPolicy(String arg0) throws SdkClientException, AmazonServiceException {
		try {
			client.deleteBucketPolicy(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			client.deleteBucketPolicy(arg0);
		}
	}

	@Override
	public void deleteBucketPolicy(DeleteBucketPolicyRequest arg0) throws SdkClientException, AmazonServiceException {
		try {
			client.deleteBucketPolicy(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			client.deleteBucketPolicy(arg0);
		}
	}

	@Override
	public void deleteBucketReplicationConfiguration(String arg0) throws AmazonServiceException, SdkClientException {
		try {
			client.deleteBucketReplicationConfiguration(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			client.deleteBucketReplicationConfiguration(arg0);
		}
	}

	@Override
	public void deleteBucketReplicationConfiguration(DeleteBucketReplicationConfigurationRequest arg0) throws AmazonServiceException, SdkClientException {
		try {
			client.deleteBucketReplicationConfiguration(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			client.deleteBucketReplicationConfiguration(arg0);
		}
	}

	@Override
	public void deleteBucketTaggingConfiguration(String arg0) {
		try {
			client.deleteBucketTaggingConfiguration(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			client.deleteBucketTaggingConfiguration(arg0);
		}
	}

	@Override
	public void deleteBucketTaggingConfiguration(DeleteBucketTaggingConfigurationRequest arg0) {
		try {
			client.deleteBucketTaggingConfiguration(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			client.deleteBucketTaggingConfiguration(arg0);
		}
	}

	@Override
	public void deleteBucketWebsiteConfiguration(String arg0) throws SdkClientException, AmazonServiceException {
		try {
			client.deleteBucketWebsiteConfiguration(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			client.deleteBucketWebsiteConfiguration(arg0);
		}
	}

	@Override
	public void deleteBucketWebsiteConfiguration(DeleteBucketWebsiteConfigurationRequest arg0) throws SdkClientException, AmazonServiceException {
		try {
			client.deleteBucketWebsiteConfiguration(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			client.deleteBucketWebsiteConfiguration(arg0);
		}
	}

	@Override
	public void deleteObject(DeleteObjectRequest arg0) throws SdkClientException, AmazonServiceException {
		try {
			client.deleteObject(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			client.deleteObject(arg0);
		}
	}

	@Override
	public void deleteObject(String arg0, String arg1) throws SdkClientException, AmazonServiceException {
		try {
			client.deleteObject(arg0, arg1);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			client.deleteObject(arg0, arg1);
		}
	}

	@Override
	public DeleteObjectTaggingResult deleteObjectTagging(DeleteObjectTaggingRequest arg0) {
		try {
			return client.deleteObjectTagging(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.deleteObjectTagging(arg0);
		}
	}

	@Override
	public DeleteObjectsResult deleteObjects(DeleteObjectsRequest arg0) throws SdkClientException, AmazonServiceException {
		try {
			return client.deleteObjects(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.deleteObjects(arg0);
		}
	}

	@Override
	public DeletePublicAccessBlockResult deletePublicAccessBlock(DeletePublicAccessBlockRequest arg0) {
		try {
			return client.deletePublicAccessBlock(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.deletePublicAccessBlock(arg0);
		}
	}

	@Override
	public void deleteVersion(DeleteVersionRequest arg0) throws SdkClientException, AmazonServiceException {
		try {
			client.deleteVersion(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			client.deleteVersion(arg0);
		}
	}

	@Override
	public void deleteVersion(String arg0, String arg1, String arg2) throws SdkClientException, AmazonServiceException {
		try {
			client.deleteVersion(arg0, arg1, arg2);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			client.deleteVersion(arg0, arg1, arg2);
		}
	}

	@Override
	public void disableRequesterPays(String arg0) throws AmazonServiceException, SdkClientException {
		try {
			client.disableRequesterPays(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			client.disableRequesterPays(arg0);
		}
	}

	@Override
	public boolean doesBucketExist(String arg0) throws SdkClientException, AmazonServiceException {
		try {
			return client.doesBucketExist(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.doesBucketExist(arg0);
		}
	}

	@Override
	public boolean doesBucketExistV2(String arg0) throws SdkClientException, AmazonServiceException {
		try {
			return client.doesBucketExistV2(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.doesBucketExistV2(arg0);
		}
	}

	@Override
	public boolean doesObjectExist(String arg0, String arg1) throws AmazonServiceException, SdkClientException {
		try {
			return client.doesObjectExist(arg0, arg1);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.doesObjectExist(arg0, arg1);
		}
	}

	@Override
	public PresignedUrlDownloadResult download(PresignedUrlDownloadRequest arg0) {
		try {
			return client.download(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.download(arg0);
		}
	}

	@Override
	public void download(PresignedUrlDownloadRequest arg0, File arg1) {
		try {
			client.download(arg0, arg1);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			client.download(arg0, arg1);
		}
	}

	@Override
	public void enableRequesterPays(String arg0) throws AmazonServiceException, SdkClientException {
		try {
			client.enableRequesterPays(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			client.enableRequesterPays(arg0);
		}
	}

	@Override
	public URL generatePresignedUrl(GeneratePresignedUrlRequest arg0) throws SdkClientException {
		try {
			return client.generatePresignedUrl(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.generatePresignedUrl(arg0);
		}
	}

	@Override
	public URL generatePresignedUrl(String arg0, String arg1, Date arg2) throws SdkClientException {
		try {
			return client.generatePresignedUrl(arg0, arg1, arg2);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.generatePresignedUrl(arg0, arg1, arg2);
		}
	}

	@Override
	public URL generatePresignedUrl(String arg0, String arg1, Date arg2, HttpMethod arg3) throws SdkClientException {
		try {
			return client.generatePresignedUrl(arg0, arg1, arg2, arg3);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.generatePresignedUrl(arg0, arg1, arg2, arg3);
		}
	}

	@Override
	public BucketAccelerateConfiguration getBucketAccelerateConfiguration(String arg0) throws AmazonServiceException, SdkClientException {
		try {
			return client.getBucketAccelerateConfiguration(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.getBucketAccelerateConfiguration(arg0);
		}
	}

	@Override
	public BucketAccelerateConfiguration getBucketAccelerateConfiguration(GetBucketAccelerateConfigurationRequest arg0) throws AmazonServiceException, SdkClientException {
		try {
			return client.getBucketAccelerateConfiguration(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.getBucketAccelerateConfiguration(arg0);
		}
	}

	@Override
	public AccessControlList getBucketAcl(String arg0) throws SdkClientException, AmazonServiceException {
		try {
			return client.getBucketAcl(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.getBucketAcl(arg0);
		}
	}

	@Override
	public AccessControlList getBucketAcl(GetBucketAclRequest arg0) throws SdkClientException, AmazonServiceException {
		try {
			return client.getBucketAcl(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.getBucketAcl(arg0);
		}
	}

	@Override
	public GetBucketAnalyticsConfigurationResult getBucketAnalyticsConfiguration(GetBucketAnalyticsConfigurationRequest arg0) throws AmazonServiceException, SdkClientException {
		try {
			return client.getBucketAnalyticsConfiguration(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.getBucketAnalyticsConfiguration(arg0);
		}
	}

	@Override
	public GetBucketAnalyticsConfigurationResult getBucketAnalyticsConfiguration(String arg0, String arg1) throws AmazonServiceException, SdkClientException {
		try {
			return client.getBucketAnalyticsConfiguration(arg0, arg1);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.getBucketAnalyticsConfiguration(arg0, arg1);
		}
	}

	@Override
	public BucketCrossOriginConfiguration getBucketCrossOriginConfiguration(String arg0) {
		try {
			return client.getBucketCrossOriginConfiguration(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.getBucketCrossOriginConfiguration(arg0);
		}
	}

	@Override
	public BucketCrossOriginConfiguration getBucketCrossOriginConfiguration(GetBucketCrossOriginConfigurationRequest arg0) {
		try {
			return client.getBucketCrossOriginConfiguration(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.getBucketCrossOriginConfiguration(arg0);
		}
	}

	@Override
	public GetBucketEncryptionResult getBucketEncryption(String arg0) throws AmazonServiceException, SdkClientException {
		try {
			return client.getBucketEncryption(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.getBucketEncryption(arg0);
		}
	}

	@Override
	public GetBucketEncryptionResult getBucketEncryption(GetBucketEncryptionRequest arg0) throws AmazonServiceException, SdkClientException {
		try {
			return client.getBucketEncryption(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.getBucketEncryption(arg0);
		}
	}

	@Override
	public GetBucketIntelligentTieringConfigurationResult getBucketIntelligentTieringConfiguration(GetBucketIntelligentTieringConfigurationRequest arg0)
			throws AmazonServiceException, SdkClientException {
		try {
			return client.getBucketIntelligentTieringConfiguration(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.getBucketIntelligentTieringConfiguration(arg0);
		}
	}

	@Override
	public GetBucketIntelligentTieringConfigurationResult getBucketIntelligentTieringConfiguration(String arg0, String arg1) throws AmazonServiceException, SdkClientException {
		try {
			return client.getBucketIntelligentTieringConfiguration(arg0, arg1);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.getBucketIntelligentTieringConfiguration(arg0, arg1);
		}
	}

	@Override
	public GetBucketInventoryConfigurationResult getBucketInventoryConfiguration(GetBucketInventoryConfigurationRequest arg0) throws AmazonServiceException, SdkClientException {
		try {
			return client.getBucketInventoryConfiguration(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.getBucketInventoryConfiguration(arg0);
		}
	}

	@Override
	public GetBucketInventoryConfigurationResult getBucketInventoryConfiguration(String arg0, String arg1) throws AmazonServiceException, SdkClientException {
		try {
			return client.getBucketInventoryConfiguration(arg0, arg1);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.getBucketInventoryConfiguration(arg0, arg1);
		}
	}

	@Override
	public BucketLifecycleConfiguration getBucketLifecycleConfiguration(String arg0) {
		try {
			return client.getBucketLifecycleConfiguration(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.getBucketLifecycleConfiguration(arg0);
		}
	}

	@Override
	public BucketLifecycleConfiguration getBucketLifecycleConfiguration(GetBucketLifecycleConfigurationRequest arg0) {
		try {
			return client.getBucketLifecycleConfiguration(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.getBucketLifecycleConfiguration(arg0);
		}
	}

	@Override
	public String getBucketLocation(String arg0) throws SdkClientException, AmazonServiceException {
		try {
			return client.getBucketLocation(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.getBucketLocation(arg0);
		}
	}

	@Override
	public String getBucketLocation(GetBucketLocationRequest arg0) throws SdkClientException, AmazonServiceException {
		try {
			return client.getBucketLocation(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.getBucketLocation(arg0);
		}
	}

	@Override
	public BucketLoggingConfiguration getBucketLoggingConfiguration(String arg0) throws SdkClientException, AmazonServiceException {
		try {
			return client.getBucketLoggingConfiguration(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.getBucketLoggingConfiguration(arg0);
		}
	}

	@Override
	public BucketLoggingConfiguration getBucketLoggingConfiguration(GetBucketLoggingConfigurationRequest arg0) throws SdkClientException, AmazonServiceException {
		try {
			return client.getBucketLoggingConfiguration(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.getBucketLoggingConfiguration(arg0);
		}
	}

	@Override
	public GetBucketMetricsConfigurationResult getBucketMetricsConfiguration(GetBucketMetricsConfigurationRequest arg0) throws AmazonServiceException, SdkClientException {
		try {
			return client.getBucketMetricsConfiguration(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.getBucketMetricsConfiguration(arg0);
		}
	}

	@Override
	public GetBucketMetricsConfigurationResult getBucketMetricsConfiguration(String arg0, String arg1) throws AmazonServiceException, SdkClientException {
		try {
			return client.getBucketMetricsConfiguration(arg0, arg1);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.getBucketMetricsConfiguration(arg0, arg1);
		}
	}

	@Override
	public BucketNotificationConfiguration getBucketNotificationConfiguration(String arg0) throws SdkClientException, AmazonServiceException {
		try {
			return client.getBucketNotificationConfiguration(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.getBucketNotificationConfiguration(arg0);
		}
	}

	@Override
	public BucketNotificationConfiguration getBucketNotificationConfiguration(GetBucketNotificationConfigurationRequest arg0) throws SdkClientException, AmazonServiceException {
		try {
			return client.getBucketNotificationConfiguration(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.getBucketNotificationConfiguration(arg0);
		}
	}

	@Override
	public GetBucketOwnershipControlsResult getBucketOwnershipControls(GetBucketOwnershipControlsRequest arg0) throws AmazonServiceException, SdkClientException {
		try {
			return client.getBucketOwnershipControls(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.getBucketOwnershipControls(arg0);
		}
	}

	@Override
	public BucketPolicy getBucketPolicy(String arg0) throws SdkClientException, AmazonServiceException {
		try {
			return client.getBucketPolicy(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.getBucketPolicy(arg0);
		}
	}

	@Override
	public BucketPolicy getBucketPolicy(GetBucketPolicyRequest arg0) throws SdkClientException, AmazonServiceException {
		try {
			return client.getBucketPolicy(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.getBucketPolicy(arg0);
		}
	}

	@Override
	public GetBucketPolicyStatusResult getBucketPolicyStatus(GetBucketPolicyStatusRequest arg0) {
		try {
			return client.getBucketPolicyStatus(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.getBucketPolicyStatus(arg0);
		}
	}

	@Override
	public BucketReplicationConfiguration getBucketReplicationConfiguration(String arg0) throws AmazonServiceException, SdkClientException {
		try {
			return client.getBucketReplicationConfiguration(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.getBucketReplicationConfiguration(arg0);
		}
	}

	@Override
	public BucketReplicationConfiguration getBucketReplicationConfiguration(GetBucketReplicationConfigurationRequest arg0) throws AmazonServiceException, SdkClientException {
		try {
			return client.getBucketReplicationConfiguration(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.getBucketReplicationConfiguration(arg0);
		}
	}

	@Override
	public BucketTaggingConfiguration getBucketTaggingConfiguration(String arg0) {
		try {
			return client.getBucketTaggingConfiguration(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.getBucketTaggingConfiguration(arg0);
		}
	}

	@Override
	public BucketTaggingConfiguration getBucketTaggingConfiguration(GetBucketTaggingConfigurationRequest arg0) {
		try {
			return client.getBucketTaggingConfiguration(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.getBucketTaggingConfiguration(arg0);
		}
	}

	@Override
	public BucketVersioningConfiguration getBucketVersioningConfiguration(String arg0) throws SdkClientException, AmazonServiceException {
		try {
			return client.getBucketVersioningConfiguration(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.getBucketVersioningConfiguration(arg0);
		}
	}

	@Override
	public BucketVersioningConfiguration getBucketVersioningConfiguration(GetBucketVersioningConfigurationRequest arg0) throws SdkClientException, AmazonServiceException {
		try {
			return client.getBucketVersioningConfiguration(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.getBucketVersioningConfiguration(arg0);
		}
	}

	@Override
	public BucketWebsiteConfiguration getBucketWebsiteConfiguration(String arg0) throws SdkClientException, AmazonServiceException {
		try {
			return client.getBucketWebsiteConfiguration(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.getBucketWebsiteConfiguration(arg0);
		}
	}

	@Override
	public BucketWebsiteConfiguration getBucketWebsiteConfiguration(GetBucketWebsiteConfigurationRequest arg0) throws SdkClientException, AmazonServiceException {
		try {
			return client.getBucketWebsiteConfiguration(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.getBucketWebsiteConfiguration(arg0);
		}
	}

	@Override
	public S3ResponseMetadata getCachedResponseMetadata(AmazonWebServiceRequest arg0) {
		try {
			return client.getCachedResponseMetadata(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.getCachedResponseMetadata(arg0);
		}
	}

	@Override
	public S3Object getObject(GetObjectRequest arg0) throws SdkClientException, AmazonServiceException {
		try {
			return client.getObject(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.getObject(arg0);
		}
	}

	@Override
	public S3Object getObject(String arg0, String arg1) throws SdkClientException, AmazonServiceException {
		try {
			return client.getObject(arg0, arg1);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.getObject(arg0, arg1);
		}
	}

	@Override
	public ObjectMetadata getObject(GetObjectRequest arg0, File arg1) throws SdkClientException, AmazonServiceException {
		try {
			return client.getObject(arg0, arg1);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.getObject(arg0, arg1);
		}
	}

	@Override
	public AccessControlList getObjectAcl(GetObjectAclRequest arg0) throws SdkClientException, AmazonServiceException {
		try {
			return client.getObjectAcl(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.getObjectAcl(arg0);
		}
	}

	@Override
	public AccessControlList getObjectAcl(String arg0, String arg1) throws SdkClientException, AmazonServiceException {
		try {
			return client.getObjectAcl(arg0, arg1);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.getObjectAcl(arg0, arg1);
		}
	}

	@Override
	public AccessControlList getObjectAcl(String arg0, String arg1, String arg2) throws SdkClientException, AmazonServiceException {
		try {
			return client.getObjectAcl(arg0, arg1, arg2);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.getObjectAcl(arg0, arg1, arg2);
		}
	}

	@Override
	public String getObjectAsString(String arg0, String arg1) throws AmazonServiceException, SdkClientException {
		try {
			return client.getObjectAsString(arg0, arg1);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.getObjectAsString(arg0, arg1);
		}
	}

	@Override
	public GetObjectLegalHoldResult getObjectLegalHold(GetObjectLegalHoldRequest arg0) {
		try {
			return client.getObjectLegalHold(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.getObjectLegalHold(arg0);
		}
	}

	@Override
	public GetObjectLockConfigurationResult getObjectLockConfiguration(GetObjectLockConfigurationRequest arg0) {
		try {
			return client.getObjectLockConfiguration(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.getObjectLockConfiguration(arg0);
		}
	}

	@Override
	public ObjectMetadata getObjectMetadata(GetObjectMetadataRequest arg0) throws SdkClientException, AmazonServiceException {
		try {
			return client.getObjectMetadata(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.getObjectMetadata(arg0);
		}
	}

	@Override
	public ObjectMetadata getObjectMetadata(String arg0, String arg1) throws SdkClientException, AmazonServiceException {
		try {
			return client.getObjectMetadata(arg0, arg1);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.getObjectMetadata(arg0, arg1);
		}
	}

	@Override
	public GetObjectRetentionResult getObjectRetention(GetObjectRetentionRequest arg0) {
		try {
			return client.getObjectRetention(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.getObjectRetention(arg0);
		}
	}

	@Override
	public GetObjectTaggingResult getObjectTagging(GetObjectTaggingRequest arg0) {
		try {
			return client.getObjectTagging(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.getObjectTagging(arg0);
		}
	}

	@Override
	public GetPublicAccessBlockResult getPublicAccessBlock(GetPublicAccessBlockRequest arg0) {
		try {
			return client.getPublicAccessBlock(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.getPublicAccessBlock(arg0);
		}
	}

	@Override
	public Region getRegion() {
		try {
			return client.getRegion();
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.getRegion();
		}
	}

	@Override
	public String getRegionName() {
		try {
			return client.getRegionName();
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.getRegionName();
		}
	}

	@Override
	public Owner getS3AccountOwner() throws SdkClientException, AmazonServiceException {
		try {
			return client.getS3AccountOwner();
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.getS3AccountOwner();
		}
	}

	@Override
	public Owner getS3AccountOwner(GetS3AccountOwnerRequest arg0) throws SdkClientException, AmazonServiceException {
		try {
			return client.getS3AccountOwner(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.getS3AccountOwner(arg0);
		}
	}

	@Override
	public URL getUrl(String arg0, String arg1) {
		try {
			return client.getUrl(arg0, arg1);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.getUrl(arg0, arg1);
		}
	}

	@Override
	public HeadBucketResult headBucket(HeadBucketRequest arg0) throws SdkClientException, AmazonServiceException {
		try {
			return client.headBucket(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.headBucket(arg0);
		}
	}

	@Override
	public InitiateMultipartUploadResult initiateMultipartUpload(InitiateMultipartUploadRequest arg0) throws SdkClientException, AmazonServiceException {
		try {
			return client.initiateMultipartUpload(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.initiateMultipartUpload(arg0);
		}
	}

	@Override
	public boolean isRequesterPaysEnabled(String arg0) throws AmazonServiceException, SdkClientException {
		try {
			return client.isRequesterPaysEnabled(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.isRequesterPaysEnabled(arg0);
		}
	}

	@Override
	public ListBucketAnalyticsConfigurationsResult listBucketAnalyticsConfigurations(ListBucketAnalyticsConfigurationsRequest arg0)
			throws AmazonServiceException, SdkClientException {
		try {
			return client.listBucketAnalyticsConfigurations(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.listBucketAnalyticsConfigurations(arg0);
		}
	}

	@Override
	public ListBucketIntelligentTieringConfigurationsResult listBucketIntelligentTieringConfigurations(ListBucketIntelligentTieringConfigurationsRequest arg0)
			throws AmazonServiceException, SdkClientException {
		try {
			return client.listBucketIntelligentTieringConfigurations(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.listBucketIntelligentTieringConfigurations(arg0);
		}
	}

	@Override
	public ListBucketInventoryConfigurationsResult listBucketInventoryConfigurations(ListBucketInventoryConfigurationsRequest arg0)
			throws AmazonServiceException, SdkClientException {
		try {
			return client.listBucketInventoryConfigurations(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.listBucketInventoryConfigurations(arg0);
		}
	}

	@Override
	public ListBucketMetricsConfigurationsResult listBucketMetricsConfigurations(ListBucketMetricsConfigurationsRequest arg0) throws AmazonServiceException, SdkClientException {
		try {
			return client.listBucketMetricsConfigurations(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.listBucketMetricsConfigurations(arg0);
		}
	}

	@Override
	public List<Bucket> listBuckets() throws SdkClientException, AmazonServiceException {
		try {
			return client.listBuckets();
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.listBuckets();
		}
	}

	@Override
	public List<Bucket> listBuckets(ListBucketsRequest arg0) throws SdkClientException, AmazonServiceException {
		try {
			return client.listBuckets(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.listBuckets(arg0);
		}
	}

	@Override
	public MultipartUploadListing listMultipartUploads(ListMultipartUploadsRequest arg0) throws SdkClientException, AmazonServiceException {
		try {
			return client.listMultipartUploads(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.listMultipartUploads(arg0);
		}
	}

	@Override
	public ObjectListing listNextBatchOfObjects(ObjectListing arg0) throws SdkClientException, AmazonServiceException {
		try {
			return client.listNextBatchOfObjects(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.listNextBatchOfObjects(arg0);
		}
	}

	@Override
	public ObjectListing listNextBatchOfObjects(ListNextBatchOfObjectsRequest arg0) throws SdkClientException, AmazonServiceException {
		try {
			return client.listNextBatchOfObjects(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.listNextBatchOfObjects(arg0);
		}
	}

	@Override
	public VersionListing listNextBatchOfVersions(VersionListing arg0) throws SdkClientException, AmazonServiceException {
		try {
			return client.listNextBatchOfVersions(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.listNextBatchOfVersions(arg0);
		}
	}

	@Override
	public VersionListing listNextBatchOfVersions(ListNextBatchOfVersionsRequest arg0) throws SdkClientException, AmazonServiceException {
		try {
			return client.listNextBatchOfVersions(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.listNextBatchOfVersions(arg0);
		}
	}

	@Override
	public ObjectListing listObjects(String arg0) throws SdkClientException, AmazonServiceException {
		try {
			return client.listObjects(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.listObjects(arg0);
		}
	}

	@Override
	public ObjectListing listObjects(ListObjectsRequest arg0) throws SdkClientException, AmazonServiceException {
		try {
			return client.listObjects(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.listObjects(arg0);
		}
	}

	@Override
	public ObjectListing listObjects(String arg0, String arg1) throws SdkClientException, AmazonServiceException {
		try {
			return client.listObjects(arg0, arg1);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.listObjects(arg0, arg1);
		}
	}

	@Override
	public ListObjectsV2Result listObjectsV2(String arg0) throws SdkClientException, AmazonServiceException {
		try {
			return client.listObjectsV2(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.listObjectsV2(arg0);
		}
	}

	@Override
	public ListObjectsV2Result listObjectsV2(ListObjectsV2Request arg0) throws SdkClientException, AmazonServiceException {
		try {
			return client.listObjectsV2(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.listObjectsV2(arg0);
		}
	}

	@Override
	public ListObjectsV2Result listObjectsV2(String arg0, String arg1) throws SdkClientException, AmazonServiceException {
		try {
			return client.listObjectsV2(arg0, arg1);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.listObjectsV2(arg0, arg1);
		}
	}

	@Override
	public PartListing listParts(ListPartsRequest arg0) throws SdkClientException, AmazonServiceException {
		try {
			return client.listParts(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.listParts(arg0);
		}
	}

	@Override
	public VersionListing listVersions(ListVersionsRequest arg0) throws SdkClientException, AmazonServiceException {
		try {
			return client.listVersions(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.listVersions(arg0);
		}
	}

	@Override
	public VersionListing listVersions(String arg0, String arg1) throws SdkClientException, AmazonServiceException {
		try {
			return client.listVersions(arg0, arg1);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.listVersions(arg0, arg1);
		}
	}

	@Override
	public VersionListing listVersions(String arg0, String arg1, String arg2, String arg3, String arg4, Integer arg5) throws SdkClientException, AmazonServiceException {
		try {
			return client.listVersions(arg0, arg1, arg2, arg3, arg4, arg5);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.listVersions(arg0, arg1, arg2, arg3, arg4, arg5);
		}
	}

	@Override
	public PutObjectResult putObject(PutObjectRequest arg0) throws SdkClientException, AmazonServiceException {
		try {
			return client.putObject(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.putObject(arg0);
		}
	}

	@Override
	public PutObjectResult putObject(String arg0, String arg1, File arg2) throws SdkClientException, AmazonServiceException {
		try {
			return client.putObject(arg0, arg1, arg2);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.putObject(arg0, arg1, arg2);
		}
	}

	@Override
	public PutObjectResult putObject(String arg0, String arg1, String arg2) throws AmazonServiceException, SdkClientException {
		try {
			return client.putObject(arg0, arg1, arg2);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.putObject(arg0, arg1, arg2);
		}
	}

	@Override
	public PutObjectResult putObject(String arg0, String arg1, InputStream arg2, ObjectMetadata arg3) throws SdkClientException, AmazonServiceException {
		try {
			return client.putObject(arg0, arg1, arg2, arg3);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.putObject(arg0, arg1, arg2, arg3);
		}
	}

	@Override
	public void restoreObject(RestoreObjectRequest arg0) throws AmazonServiceException {
		try {
			client.restoreObject(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			client.restoreObject(arg0);
		}
	}

	@Override
	public void restoreObject(String arg0, String arg1, int arg2) throws AmazonServiceException {
		try {
			client.restoreObject(arg0, arg1, arg2);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			client.restoreObject(arg0, arg1, arg2);
		}
	}

	@Override
	public RestoreObjectResult restoreObjectV2(RestoreObjectRequest arg0) throws AmazonServiceException {
		try {
			return client.restoreObjectV2(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.restoreObjectV2(arg0);
		}
	}

	@Override
	public SelectObjectContentResult selectObjectContent(SelectObjectContentRequest arg0) throws AmazonServiceException, SdkClientException {
		try {
			return client.selectObjectContent(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.selectObjectContent(arg0);
		}
	}

	@Override
	public void setBucketAccelerateConfiguration(SetBucketAccelerateConfigurationRequest arg0) throws AmazonServiceException, SdkClientException {
		try {
			client.setBucketAccelerateConfiguration(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			client.setBucketAccelerateConfiguration(arg0);
		}
	}

	@Override
	public void setBucketAccelerateConfiguration(String arg0, BucketAccelerateConfiguration arg1) throws AmazonServiceException, SdkClientException {
		try {
			client.setBucketAccelerateConfiguration(arg0, arg1);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			client.setBucketAccelerateConfiguration(arg0, arg1);
		}
	}

	@Override
	public void setBucketAcl(SetBucketAclRequest arg0) throws SdkClientException, AmazonServiceException {
		try {
			client.setBucketAcl(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			client.setBucketAcl(arg0);
		}
	}

	@Override
	public void setBucketAcl(String arg0, AccessControlList arg1) throws SdkClientException, AmazonServiceException {
		try {
			client.setBucketAcl(arg0, arg1);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			client.setBucketAcl(arg0, arg1);
		}
	}

	@Override
	public void setBucketAcl(String arg0, CannedAccessControlList arg1) throws SdkClientException, AmazonServiceException {
		try {
			client.setBucketAcl(arg0, arg1);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			client.setBucketAcl(arg0, arg1);
		}
	}

	@Override
	public SetBucketAnalyticsConfigurationResult setBucketAnalyticsConfiguration(SetBucketAnalyticsConfigurationRequest arg0) throws AmazonServiceException, SdkClientException {
		try {
			return client.setBucketAnalyticsConfiguration(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.setBucketAnalyticsConfiguration(arg0);
		}
	}

	@Override
	public SetBucketAnalyticsConfigurationResult setBucketAnalyticsConfiguration(String arg0, AnalyticsConfiguration arg1) throws AmazonServiceException, SdkClientException {
		try {
			return client.setBucketAnalyticsConfiguration(arg0, arg1);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.setBucketAnalyticsConfiguration(arg0, arg1);
		}
	}

	@Override
	public void setBucketCrossOriginConfiguration(SetBucketCrossOriginConfigurationRequest arg0) {
		try {
			client.setBucketCrossOriginConfiguration(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			client.setBucketCrossOriginConfiguration(arg0);
		}
	}

	@Override
	public void setBucketCrossOriginConfiguration(String arg0, BucketCrossOriginConfiguration arg1) {
		try {
			client.setBucketCrossOriginConfiguration(arg0, arg1);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			client.setBucketCrossOriginConfiguration(arg0, arg1);
		}
	}

	@Override
	public SetBucketEncryptionResult setBucketEncryption(SetBucketEncryptionRequest arg0) throws AmazonServiceException, SdkClientException {
		try {
			return client.setBucketEncryption(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.setBucketEncryption(arg0);
		}
	}

	@Override
	public SetBucketIntelligentTieringConfigurationResult setBucketIntelligentTieringConfiguration(SetBucketIntelligentTieringConfigurationRequest arg0)
			throws AmazonServiceException, SdkClientException {
		try {
			return client.setBucketIntelligentTieringConfiguration(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.setBucketIntelligentTieringConfiguration(arg0);
		}
	}

	@Override
	public SetBucketIntelligentTieringConfigurationResult setBucketIntelligentTieringConfiguration(String arg0, IntelligentTieringConfiguration arg1)
			throws AmazonServiceException, SdkClientException {
		try {
			return client.setBucketIntelligentTieringConfiguration(arg0, arg1);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.setBucketIntelligentTieringConfiguration(arg0, arg1);
		}
	}

	@Override
	public SetBucketInventoryConfigurationResult setBucketInventoryConfiguration(SetBucketInventoryConfigurationRequest arg0) throws AmazonServiceException, SdkClientException {
		try {
			return client.setBucketInventoryConfiguration(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.setBucketInventoryConfiguration(arg0);
		}
	}

	@Override
	public SetBucketInventoryConfigurationResult setBucketInventoryConfiguration(String arg0, InventoryConfiguration arg1) throws AmazonServiceException, SdkClientException {
		try {
			return client.setBucketInventoryConfiguration(arg0, arg1);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.setBucketInventoryConfiguration(arg0, arg1);
		}
	}

	@Override
	public void setBucketLifecycleConfiguration(SetBucketLifecycleConfigurationRequest arg0) {
		try {
			client.setBucketLifecycleConfiguration(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			client.setBucketLifecycleConfiguration(arg0);
		}
	}

	@Override
	public void setBucketLifecycleConfiguration(String arg0, BucketLifecycleConfiguration arg1) {
		try {
			client.setBucketLifecycleConfiguration(arg0, arg1);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			client.setBucketLifecycleConfiguration(arg0, arg1);
		}
	}

	@Override
	public void setBucketLoggingConfiguration(SetBucketLoggingConfigurationRequest arg0) throws SdkClientException, AmazonServiceException {
		try {
			client.setBucketLoggingConfiguration(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			client.setBucketLoggingConfiguration(arg0);
		}
	}

	@Override
	public SetBucketMetricsConfigurationResult setBucketMetricsConfiguration(SetBucketMetricsConfigurationRequest arg0) throws AmazonServiceException, SdkClientException {
		try {
			return client.setBucketMetricsConfiguration(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.setBucketMetricsConfiguration(arg0);
		}
	}

	@Override
	public SetBucketMetricsConfigurationResult setBucketMetricsConfiguration(String arg0, MetricsConfiguration arg1) throws AmazonServiceException, SdkClientException {
		try {
			return client.setBucketMetricsConfiguration(arg0, arg1);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.setBucketMetricsConfiguration(arg0, arg1);
		}
	}

	@Override
	public void setBucketNotificationConfiguration(SetBucketNotificationConfigurationRequest arg0) throws SdkClientException, AmazonServiceException {
		try {
			client.setBucketNotificationConfiguration(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			client.setBucketNotificationConfiguration(arg0);
		}
	}

	@Override
	public void setBucketNotificationConfiguration(String arg0, BucketNotificationConfiguration arg1) throws SdkClientException, AmazonServiceException {
		try {
			client.setBucketNotificationConfiguration(arg0, arg1);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			client.setBucketNotificationConfiguration(arg0, arg1);
		}
	}

	@Override
	public SetBucketOwnershipControlsResult setBucketOwnershipControls(SetBucketOwnershipControlsRequest arg0) throws AmazonServiceException, SdkClientException {
		try {
			return client.setBucketOwnershipControls(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.setBucketOwnershipControls(arg0);
		}
	}

	@Override
	public SetBucketOwnershipControlsResult setBucketOwnershipControls(String arg0, OwnershipControls arg1) throws AmazonServiceException, SdkClientException {
		try {
			return client.setBucketOwnershipControls(arg0, arg1);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.setBucketOwnershipControls(arg0, arg1);
		}
	}

	@Override
	public void setBucketPolicy(SetBucketPolicyRequest arg0) throws SdkClientException, AmazonServiceException {
		try {
			client.setBucketPolicy(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			client.setBucketPolicy(arg0);
		}
	}

	@Override
	public void setBucketPolicy(String arg0, String arg1) throws SdkClientException, AmazonServiceException {
		try {
			client.setBucketPolicy(arg0, arg1);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			client.setBucketPolicy(arg0, arg1);
		}
	}

	@Override
	public void setBucketReplicationConfiguration(SetBucketReplicationConfigurationRequest arg0) throws AmazonServiceException, SdkClientException {
		try {
			client.setBucketReplicationConfiguration(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			client.setBucketReplicationConfiguration(arg0);
		}
	}

	@Override
	public void setBucketReplicationConfiguration(String arg0, BucketReplicationConfiguration arg1) throws AmazonServiceException, SdkClientException {
		try {
			client.setBucketReplicationConfiguration(arg0, arg1);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			client.setBucketReplicationConfiguration(arg0, arg1);
		}
	}

	@Override
	public void setBucketTaggingConfiguration(SetBucketTaggingConfigurationRequest arg0) {
		try {
			client.setBucketTaggingConfiguration(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			client.setBucketTaggingConfiguration(arg0);
		}
	}

	@Override
	public void setBucketTaggingConfiguration(String arg0, BucketTaggingConfiguration arg1) {
		try {
			client.setBucketTaggingConfiguration(arg0, arg1);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			client.setBucketTaggingConfiguration(arg0, arg1);
		}
	}

	@Override
	public void setBucketVersioningConfiguration(SetBucketVersioningConfigurationRequest arg0) throws SdkClientException, AmazonServiceException {
		try {
			client.setBucketVersioningConfiguration(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			client.setBucketVersioningConfiguration(arg0);
		}
	}

	@Override
	public void setBucketWebsiteConfiguration(SetBucketWebsiteConfigurationRequest arg0) throws SdkClientException, AmazonServiceException {
		try {
			client.setBucketWebsiteConfiguration(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			client.setBucketWebsiteConfiguration(arg0);
		}
	}

	@Override
	public void setBucketWebsiteConfiguration(String arg0, BucketWebsiteConfiguration arg1) throws SdkClientException, AmazonServiceException {
		try {
			client.setBucketWebsiteConfiguration(arg0, arg1);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			client.setBucketWebsiteConfiguration(arg0, arg1);
		}
	}

	@Override
	public void setEndpoint(String arg0) {
		try {
			client.setEndpoint(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			client.setEndpoint(arg0);
		}
	}

	@Override
	public void setObjectAcl(SetObjectAclRequest arg0) throws SdkClientException, AmazonServiceException {
		try {
			client.setObjectAcl(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			client.setObjectAcl(arg0);
		}
	}

	@Override
	public void setObjectAcl(String arg0, String arg1, AccessControlList arg2) throws SdkClientException, AmazonServiceException {
		try {
			client.setObjectAcl(arg0, arg1, arg2);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			client.setObjectAcl(arg0, arg1, arg2);
		}
	}

	@Override
	public void setObjectAcl(String arg0, String arg1, CannedAccessControlList arg2) throws SdkClientException, AmazonServiceException {
		try {
			client.setObjectAcl(arg0, arg1, arg2);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			client.setObjectAcl(arg0, arg1, arg2);
		}
	}

	@Override
	public void setObjectAcl(String arg0, String arg1, String arg2, AccessControlList arg3) throws SdkClientException, AmazonServiceException {
		try {
			client.setObjectAcl(arg0, arg1, arg2, arg3);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			client.setObjectAcl(arg0, arg1, arg2, arg3);
		}
	}

	@Override
	public void setObjectAcl(String arg0, String arg1, String arg2, CannedAccessControlList arg3) throws SdkClientException, AmazonServiceException {
		try {
			client.setObjectAcl(arg0, arg1, arg2, arg3);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			client.setObjectAcl(arg0, arg1, arg2, arg3);
		}
	}

	@Override
	public SetObjectLegalHoldResult setObjectLegalHold(SetObjectLegalHoldRequest arg0) {
		try {
			return client.setObjectLegalHold(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.setObjectLegalHold(arg0);
		}
	}

	@Override
	public SetObjectLockConfigurationResult setObjectLockConfiguration(SetObjectLockConfigurationRequest arg0) {
		try {
			return client.setObjectLockConfiguration(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.setObjectLockConfiguration(arg0);
		}
	}

	@Override
	public void setObjectRedirectLocation(String arg0, String arg1, String arg2) throws SdkClientException, AmazonServiceException {
		try {
			client.setObjectRedirectLocation(arg0, arg1, arg2);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			client.setObjectRedirectLocation(arg0, arg1, arg2);
		}
	}

	@Override
	public SetObjectRetentionResult setObjectRetention(SetObjectRetentionRequest arg0) {
		try {
			return client.setObjectRetention(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.setObjectRetention(arg0);
		}
	}

	@Override
	public SetObjectTaggingResult setObjectTagging(SetObjectTaggingRequest arg0) {
		try {
			return client.setObjectTagging(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.setObjectTagging(arg0);
		}
	}

	@Override
	public SetPublicAccessBlockResult setPublicAccessBlock(SetPublicAccessBlockRequest arg0) {
		try {
			return client.setPublicAccessBlock(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.setPublicAccessBlock(arg0);
		}
	}

	@Override
	public void setRegion(com.amazonaws.regions.Region arg0) throws IllegalArgumentException {
		try {
			client.setRegion(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			client.setRegion(arg0);
		}
	}

	@Override
	public void setRequestPaymentConfiguration(SetRequestPaymentConfigurationRequest arg0) {
		try {
			client.setRequestPaymentConfiguration(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			client.setRequestPaymentConfiguration(arg0);
		}
	}

	@Override
	public void setS3ClientOptions(S3ClientOptions arg0) {
		try {
			client.setS3ClientOptions(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			client.setS3ClientOptions(arg0);
		}
	}

	@Override
	public void shutdown() {
		try {
			client.shutdown();
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			client.shutdown();
		}
	}

	@Override
	public PresignedUrlUploadResult upload(PresignedUrlUploadRequest arg0) {
		try {
			return client.upload(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.upload(arg0);
		}
	}

	@Override
	public UploadPartResult uploadPart(UploadPartRequest arg0) throws SdkClientException, AmazonServiceException {
		try {
			return client.uploadPart(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.uploadPart(arg0);
		}
	}

	@Override
	public AmazonS3Waiters waiters() {
		try {
			return client.waiters();
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.waiters();
		}
	}

	@Override
	public WriteGetObjectResponseResult writeGetObjectResponse(WriteGetObjectResponseRequest arg0) {
		try {
			return client.writeGetObjectResponse(arg0);
		}
		catch (IllegalStateException ise) {
			invalidateAmazonS3(ise);
			return client.writeGetObjectResponse(arg0);
		}
	}

	private void invalidateAmazonS3(IllegalStateException ise) throws AmazonS3Exception {
		if (log != null) log.log(Log.LEVEL_WARN, "S3", ise);
		try {
			client = create();
		}
		catch (Exception e) {
			if (log != null) log.error("S3", e);
			throw new AmazonS3Exception("failed to invalidate client", e);
		}
	}

	public void release() throws S3Exception {
		// FUTURE remove method
	}

}
