package org.lucee.extension.resource.s3;

import org.lucee.extension.resource.s3.acl.ACLList;

import lucee.loader.util.Util;

public class S3Properties {

	private String host = S3.DEFAULT_HOST;
	private String secretAccessKey;
	private String accessKeyId;
	private boolean hasCustomCredentials;
	private ACLList acl;
	private String location;

	public void setHost(String host) {
		if (Util.isEmpty(host, true)) return;
		this.host = host.trim();
	}

	public String getHost() {
		return host;
	}

	public void setSecretAccessKey(String secretAccessKey) {
		if (Util.isEmpty(secretAccessKey, true)) return;
		this.secretAccessKey = secretAccessKey.trim();
	}

	public String getSecretAccessKey() {
		return secretAccessKey;
	}

	public void setAccessKeyId(String accessKeyId) {
		if (Util.isEmpty(accessKeyId, true)) return;
		this.accessKeyId = accessKeyId.trim();
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
		return new StringBuilder().append("host:").append(host).append(";").append("accessKeyId:").append(accessKeyId).append(";").append("secretAccessKey:")
				.append(secretAccessKey).append(";").append("custom:").append(hasCustomCredentials).append(";").toString();
	}

	public void setACL(ACLList acl) {
		this.acl = acl;
	}

	public ACLList getACL() {
		return this.acl;
	}

	public String getLocation() {
		return location;
	}

	public void setLocation(String location) {
		if (Util.isEmpty(location, true)) return;
		this.location = location.trim();
	}
}
