package org.lucee.extension.resource.s3;

import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;

import com.amazonaws.services.s3.Headers;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;

/**
 * Optional properties for an object that is written or copied: user metadata, the standard HTTP
 * headers that are stored with an object and the storage class. Everything that is not set (null)
 * is left to the default behaviour of the operation.
 */
public final class ObjectOptions {

	private static final String META_PREFIX = "x-amz-meta-";
	private static final String KMS_KEY_HEADER = "x-amz-server-side-encryption-aws-kms-key-id";

	private Map<String, String> userMetadata;
	private String contentType;
	private String cacheControl;
	private String contentDisposition;
	private String contentEncoding;
	private String contentLanguage;
	private Date expires;
	private String storageClass;

	/**
	 * @param metadata user metadata, null if not defined. An empty map is a valid definition (it removes
	 *            the user metadata of the source when copying).
	 */
	public void setUserMetadata(Map<String, String> metadata) {
		if (metadata == null) {
			this.userMetadata = null;
			return;
		}
		Map<String, String> map = new LinkedHashMap<String, String>();
		for (Map.Entry<String, String> e: metadata.entrySet()) {
			String key = e.getKey() == null ? "" : e.getKey().trim();
			if (key.toLowerCase().startsWith(META_PREFIX)) key = key.substring(META_PREFIX.length());
			if (key.length() == 0) throw new IllegalArgumentException("a metadata key cannot be empty");
			if (e.getValue() == null) throw new IllegalArgumentException("the value of the metadata key [" + key + "] cannot be null");
			map.put(key, e.getValue());
		}
		this.userMetadata = map;
	}

	public Map<String, String> getUserMetadata() {
		return userMetadata;
	}

	public void setContentType(String contentType) {
		this.contentType = contentType;
	}

	public String getContentType() {
		return contentType;
	}

	public void setCacheControl(String cacheControl) {
		this.cacheControl = cacheControl;
	}

	public void setContentDisposition(String contentDisposition) {
		this.contentDisposition = contentDisposition;
	}

	public void setContentEncoding(String contentEncoding) {
		this.contentEncoding = contentEncoding;
	}

	public void setContentLanguage(String contentLanguage) {
		this.contentLanguage = contentLanguage;
	}

	public void setExpires(Date expires) {
		this.expires = expires;
	}

	public void setStorageClass(String storageClass) {
		this.storageClass = storageClass;
	}

	public String getStorageClass() {
		return storageClass;
	}

	/**
	 * @return true if the object metadata (and not only the storage class) is defined
	 */
	public boolean hasMetaData() {
		return userMetadata != null || contentType != null || cacheControl != null || contentDisposition != null || contentEncoding != null || contentLanguage != null
				|| expires != null;
	}

	public ObjectOptions copy() {
		ObjectOptions c = new ObjectOptions();
		c.userMetadata = userMetadata == null ? null : new LinkedHashMap<String, String>(userMetadata);
		c.contentType = contentType;
		c.cacheControl = cacheControl;
		c.contentDisposition = contentDisposition;
		c.contentEncoding = contentEncoding;
		c.contentLanguage = contentLanguage;
		c.expires = expires;
		c.storageClass = storageClass;
		return c;
	}

	/**
	 * @return the given options, with the content type set to the given value in case the options do
	 *         not define one themselves. The given options are not modified.
	 */
	public static ObjectOptions withDefaultContentType(ObjectOptions opts, String contentType) {
		if (contentType == null || (opts != null && opts.contentType != null)) return opts;
		ObjectOptions c = opts == null ? new ObjectOptions() : opts.copy();
		c.contentType = contentType;
		return c;
	}

	/**
	 * Applies the metadata defined to the metadata of an object that is written. Whatever is not
	 * defined is not touched.
	 */
	public void applyTo(ObjectMetadata md) {
		if (userMetadata != null) {
			for (Map.Entry<String, String> e: userMetadata.entrySet()) {
				md.addUserMetadata(e.getKey(), e.getValue());
			}
		}
		if (contentType != null) md.setContentType(contentType);
		if (cacheControl != null) md.setCacheControl(cacheControl);
		if (contentDisposition != null) md.setContentDisposition(contentDisposition);
		if (contentEncoding != null) md.setContentEncoding(contentEncoding);
		if (contentLanguage != null) md.setContentLanguage(contentLanguage);
		if (expires != null) md.setHttpExpiresDate(expires);
	}

	public void applyTo(PutObjectRequest por) {
		if (storageClass != null) por.setStorageClass(storageClass);
	}

	public void applyTo(InitiateMultipartUploadRequest request) {
		if (storageClass != null) request.withStorageClass(storageClass);
	}

	public void applyTo(CopyObjectRequest cor) {
		if (storageClass != null) cor.setStorageClass(storageClass);
	}

	/**
	 * Builds the metadata for a copy that replaces the metadata of the source (S3 does not merge, a copy
	 * with new metadata sends all of it). Everything the options do not define is taken over from the
	 * source object, so that only what is defined changes.
	 */
	public ObjectMetadata mergeInto(ObjectMetadata source) {
		ObjectMetadata md = new ObjectMetadata();
		if (source != null) {
			if (source.getContentType() != null) md.setContentType(source.getContentType());
			if (source.getCacheControl() != null) md.setCacheControl(source.getCacheControl());
			if (source.getContentDisposition() != null) md.setContentDisposition(source.getContentDisposition());
			if (source.getContentEncoding() != null) md.setContentEncoding(source.getContentEncoding());
			if (source.getContentLanguage() != null) md.setContentLanguage(source.getContentLanguage());
			if (source.getHttpExpiresDate() != null) md.setHttpExpiresDate(source.getHttpExpiresDate());
			if (source.getSSEAlgorithm() != null) md.setSSEAlgorithm(source.getSSEAlgorithm());
			Object kms = source.getRawMetadataValue(KMS_KEY_HEADER);
			if (kms != null) md.setHeader(KMS_KEY_HEADER, kms);
			Object redirect = source.getRawMetadataValue(Headers.REDIRECT_LOCATION);
			if (redirect != null) md.setHeader(Headers.REDIRECT_LOCATION, redirect);
			if (userMetadata == null && source.getUserMetadata() != null) {
				for (Map.Entry<String, String> e: source.getUserMetadata().entrySet()) {
					md.addUserMetadata(e.getKey(), e.getValue());
				}
			}
		}
		applyTo(md);
		return md;
	}
}
