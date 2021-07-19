package org.lucee.extension.resource.s3;

import org.jets3t.service.acl.AccessControlList;

import lucee.runtime.type.dt.TimeSpan;

public class S3Properties {

	private String host = S3.DEFAULT_HOST;
	private String secretAccessKey;
	private String accessKeyId;
	private boolean hasCustomCredentials;
	private AccessControlList acl;
	private String location;
	private TimeSpan cache;
	private String mappingName;

	public void setHost(String host) {
		this.host = host;
	}

	public String getHost() {
		return host;
	}

	public void setSecretAccessKey(String secretAccessKey) {
		this.secretAccessKey = secretAccessKey;
	}

	public String getSecretAccessKey() {
		return secretAccessKey;
	}

	public void setAccessKeyId(String accessKeyId) {
		this.accessKeyId = accessKeyId;
	}

	public String getAccessKeyId() {
		return accessKeyId;
	}

	public void setCustomCredentials(boolean hasCustomCredentials) {
		this.hasCustomCredentials = hasCustomCredentials;
	}

	public boolean getCustomCredentials() {
		return hasCustomCredentials;
	}

	@Override
	public String toString() {
		return new StringBuilder().append("host:").append(host).append(";accessKeyId:").append(accessKeyId).append(";secretAccessKey:").append(secretAccessKey).append(";custom:")
				.append(hasCustomCredentials).append(";mapping:").append(mappingName).append(";location:").append(location).append(";").toString();
	}

	public void setACL(AccessControlList acl) {
		this.acl = acl;
	}

	public AccessControlList getACL() {
		return this.acl;
	}

	public void setLocation(String location) {
		this.location = location;
	}

	public String getLocation() {
		return location;
	}

	public void setCache(TimeSpan cache) {
		this.cache = cache;
	}

	public TimeSpan getCache() {
		return cache;
	}

	public void setMappingName(String mappingName) {
		this.mappingName = mappingName;
	}

	public String getMappingName() {
		return this.mappingName;
	}
}
