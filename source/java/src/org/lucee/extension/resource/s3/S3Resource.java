/**
 * Copyright (c) 2015, Lucee Assosication Switzerland
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either 
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public 
 * License along with this library.  If not, see <http://www.gnu.org/licenses/>.
 * 
 */
package org.lucee.extension.resource.s3;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.List;

import org.lucee.extension.resource.ResourceSupport;
import org.lucee.extension.resource.s3.info.S3Info;

import lucee.commons.io.res.Resource;
import lucee.commons.io.res.ResourceProvider;
import lucee.loader.engine.CFMLEngine;
import lucee.loader.engine.CFMLEngineFactory;
import lucee.loader.util.Util;
import lucee.runtime.exp.PageException;
import lucee.runtime.type.Array;
import lucee.runtime.type.Struct;

public final class S3Resource extends ResourceSupport {

	private static final long serialVersionUID = 2265457088552587701L;

	private static final long FUTURE = 50000000000000L;

	private final S3ResourceProvider provider;
	private final String bucketName;
	private String objectName;
	private final S3 s3;
	private final S3Properties props;
	long infoLastAccessw = 0;
	private String location = null;
	private Object acl;// ="public-read";

	private S3Resource(CFMLEngine engine, S3 s3, S3Properties props, String location, S3ResourceProvider provider, String buckedName, String objectName) {
		super(engine);
		this.s3 = s3;
		this.props = props;
		this.provider = provider;
		this.bucketName = buckedName;
		this.objectName = objectName;
		this.location = S3.improveLocation(location);
		this.acl = props.getACL();
	}

	S3Resource(CFMLEngine engine, S3 s3, S3Properties props, String location, S3ResourceProvider provider, String path) {
		super(engine);
		this.s3 = s3;
		this.props = props;
		this.provider = provider;
		this.acl = props.getACL();
		this.location = location;
		String[] bo = toBO(path);
		this.bucketName = bo[0];
		this.objectName = bo[1];
	}

	public static String[] toBO(String path) {
		CFMLEngine engine = CFMLEngineFactory.getInstance();
		if (path.equals("/") || engine.getStringUtil().isEmpty(path, true)) {
			return new String[] { null, "" };
		}

		path = engine.getResourceUtil().translatePath(path, true, false);
		String[] arr = null;
		try {
			arr = engine.getListUtil().toStringArray(engine.getListUtil().toArrayRemoveEmpty(path, "/"));
		}
		catch (PageException e) {
			// that should never happen, because we have string as base!!!
		}
		String bucketName = arr[0];
		String objectName = null;
		for (int i = 1; i < arr.length; i++) {
			if (Util.isEmpty(objectName)) objectName = arr[i];
			else objectName += "/" + arr[i];
		}
		if (objectName == null) objectName = "";
		return new String[] { bucketName, objectName };
	}

	@Override
	public void createDirectory(boolean createParentWhenNotExists) throws IOException {
		if (isRoot()) throw new S3Exception("You cannot manipulate the root of the S3 Service!");

		engine.getResourceUtil().checkCreateDirectoryOK(this, createParentWhenNotExists);

		try {
			provider.lock(this);
			if (isBucket()) {
				s3.createDirectory(bucketName, acl, location);
			}
			else s3.createDirectory(bucketName, objectName + "/", acl, location);
		}
		catch (IOException ioe) {
			throw ioe;
		}
		finally {
			provider.unlock(this);
		}
	}

	@Override
	public void createFile(boolean createParentWhenNotExists) throws IOException {
		if (isRoot()) throw new S3Exception("You cannot manipulate the root of the S3 Service!");

		engine.getResourceUtil().checkCreateFileOK(this, createParentWhenNotExists);
		if (isBucket()) throw new IOException("Can't create file [" + getPath() + "], on this level (Bucket Level) you can only create directories");
		try {
			provider.lock(this);
			s3.createFile(bucketName, objectName, acl, location);
		}
		finally {
			provider.unlock(this);
		}
	}

	@Override
	public InputStream getInputStream() throws IOException {
		engine.getResourceUtil().checkGetInputStreamOK(this);
		provider.read(this);
		return engine.getIOUtil().toBufferedInputStream(s3.getInputStream(bucketName, objectName));
	}

	@Override
	public int getMode() {
		return 777;
	}

	@Override
	public String getName() {
		if (isRoot()) return "";
		if (isBucket()) return bucketName;
		return objectName.substring(objectName.lastIndexOf('/') + 1);
	}

	@Override
	public boolean isAbsolute() {
		return true;
	}

	@Override
	public String getPath() {
		String ip = getInnerPath();
		if (ip.length() == 1 && ip.charAt(0) == '/') return getPrefix();
		return getPrefix().concat(ip);
	}

	private String getPrefix() {

		String aki = s3.getAccessKeyId();
		String sak = s3.getSecretAccessKey();

		StringBuilder sb = new StringBuilder(provider.getScheme()).append("://");
		boolean doHost = s3.getCustomHost() && !s3.getHost().equals(S3.DEFAULT_HOST) && s3.getHost().length() > 0;
		boolean hasAt = false;
		if (s3.getCustomCredentials() && !engine.getStringUtil().isEmpty(aki)) {
			sb.append(aki);
			if (!engine.getStringUtil().isEmpty(sak)) {
				sb.append(":").append(sak);
				if (!Util.isEmpty(location)) {
					sb.append(":").append(S3.improveLocation(location));
				}
			}
			sb.append('@');
			hasAt = true;
		}
		else if (!Util.isEmpty(props.getMapping())) {
			sb.append(props.getMapping()).append('@');
			hasAt = true;
		}

		if (doHost) {
			if (!hasAt) sb.append('@');
			sb.append(s3.getHost());
		}

		return sb.toString();
	}

	@Override
	public String getParent() {
		if (isRoot()) return null;
		return getPrefix().concat(getInnerParent());
	}

	private String getInnerPath() {
		if (isRoot()) return "/";
		return engine.getResourceUtil().translatePath(bucketName + "/" + objectName, true, false);
	}

	private String getInnerParent() {
		if (isRoot()) return null;
		if (Util.isEmpty(objectName)) return "/";
		if (objectName.indexOf('/') == -1) return "/" + bucketName;
		String tmp = objectName.substring(0, objectName.lastIndexOf('/'));
		return engine.getResourceUtil().translatePath(bucketName + "/" + tmp, true, false);
	}

	@Override
	public Resource getParentResource() {
		if (isRoot()) return null;
		return new S3Resource(engine, s3, props, isBucket() ? null : location, provider, getInnerParent());// MUST direkter machen
	}

	public S3Resource getBucket() {
		if (isRoot()) return null;
		return new S3Resource(engine, s3, props, isBucket() ? null : location, provider, "/" + bucketName);
	}

	public boolean isRoot() {
		return bucketName == null;
	}

	public boolean isBucket() {
		return bucketName != null && Util.isEmpty(objectName);
	}

	@Override
	public String toString() {
		return getPath();
	}

	@Override
	public OutputStream getOutputStream(boolean append) throws IOException {

		if (isBucket()) throw new IOException("Bucket is mandatory when writing a file to S3  [" + getPath() + "]");
		if (isDirectory()) throw new IOException("Can't write to file [" + getPath() + "], as it's an existing directory.");

		if (!isRoot() && !isBucket()) {
			S3Resource bucket = getBucket();
			if (bucket != null) {
				if (!bucket.exists()) throw new IOException("Can't write file [" + getPath() + "], as bucket [" + bucket.getPath() + "] doesn't exist or cannot be accessed");
			}
		}

		try {
			byte[] barr = null;
			if (append) {
				InputStream is = null;
				OutputStream os = null;
				try {
					ByteArrayOutputStream baos = new ByteArrayOutputStream();
					os = baos;
					engine.getIOUtil().copy(is = getInputStream(), baos, false, false);
					barr = baos.toByteArray();
				}
				finally {
					Util.closeEL(is);
					Util.closeEL(os);
				}
			}

			S3ResourceOutputStream os = new S3ResourceOutputStream(s3, bucketName, objectName, getInnerPath(), acl, location);
			if (append && !(barr == null || barr.length == 0)) engine.getIOUtil().copy(new ByteArrayInputStream(barr), os, true, false);
			return os;
		}
		catch (IOException e) {
			throw e;
		}
	}

	@Override
	public Resource getRealResource(String realpath) {
		realpath = engine.getResourceUtil().merge(getInnerPath(), realpath);
		if (realpath.startsWith("../")) return null;
		return new S3Resource(engine, s3, props, location, provider, realpath);
	}

	@Override
	public ResourceProvider getResourceProvider() {
		return provider;
	}

	@Override
	public boolean isDirectory() {
		if (isRoot()) return true;
		try {
			if (isBucket()) return s3.exists(bucketName);
			else return s3.isDirectory(bucketName, objectName + "/");
		}
		catch (IOException ioe) {
			CFMLEngine e = CFMLEngineFactory.getInstance();
			throw e.getCastUtil().toPageRuntimeException(ioe);
		}
	}

	@Override
	public boolean isFile() {
		if (isRoot()) return false;
		try {
			if (isBucket()) return false;
			else return s3.isFile(bucketName, objectName);
		}
		catch (IOException ioe) {
			CFMLEngine e = CFMLEngineFactory.getInstance();
			throw e.getCastUtil().toPageRuntimeException(ioe);
		}
	}

	@Override
	public boolean exists() {
		if (isRoot()) return true;
		try {
			if (isBucket()) return s3.exists(bucketName);
			else return s3.exists(bucketName, objectName);
		}
		catch (IOException ioe) {
			CFMLEngine e = CFMLEngineFactory.getInstance();
			throw e.getCastUtil().toPageRuntimeException(ioe);
		}
	}

	@Override
	public boolean isReadable() {
		return exists();
	}

	@Override
	public boolean isWriteable() {
		return exists();
	}

	@Override
	public long lastModified() {
		if (isRoot()) return 0;
		S3Info info = getInfo();
		if (info == null) return 0;
		return info.getLastModified();
	}

	@Override
	public long length() {
		if (isRoot()) return 0;
		S3Info info = getInfo();
		if (info == null) return 0;
		return info.getSize();
	}

	private S3Info getInfo() {
		try {
			S3Info info;
			if (isBucket()) info = s3.get(bucketName);
			else info = s3.get(bucketName, objectName + "/");
			return info;
		}
		catch (IOException ioe) {
			CFMLEngine e = CFMLEngineFactory.getInstance();
			throw e.getCastUtil().toPageRuntimeException(ioe);
		}
	}

	@Override
	public Resource[] listResources() {
		S3Resource[] children = null;
		// long timeout=System.currentTimeMillis()+provider.getCache();
		try {
			boolean buckets = false;
			List<S3Info> list = null;
			if (isRoot()) {
				buckets = true;
				list = s3.list(false, false);
			}
			else if (isDirectory()) {
				list = isBucket() ? s3.list(bucketName, "", false, true, true) : s3.list(bucketName, objectName + "/", false, true, true);
			}

			if (list != null) {
				Iterator<S3Info> it = list.iterator();
				children = new S3Resource[list.size()];
				S3Info si;
				int index = 0;
				while (it.hasNext()) {
					si = it.next();
					children[index] = new S3Resource(engine, s3, props, location, provider, si.getBucketName(), buckets ? "" : S3.improveObjectName(si.getObjectName(), false));
					index++;
				}
			}
		}
		catch (S3Exception e) {
			throw CFMLEngineFactory.getInstance().getCastUtil().toPageRuntimeException(e);
		}
		return children;
	}

	@Override
	public void remove(boolean force) throws IOException {
		if (isRoot()) throw new IOException("Can not remove root of S3 Service");

		engine.getResourceUtil().checkRemoveOK(this);
		if (isBucket()) {
			s3.delete(bucketName, force);
		}
		else {
			s3.delete(bucketName, isDirectory() ? objectName + "/" : objectName, force);
		}
	}

	@Override
	public boolean setLastModified(long time) {
		// s3.releaseCache(getInnerPath());
		// TODO
		return false;
	}

	@Override
	public void setMode(int mode) throws IOException {
		// s3.releaseCache(getInnerPath());
		// TODO

	}

	@Override
	public boolean setReadable(boolean readable) {
		// s3.releaseCache(getInnerPath());
		// TODO
		return false;
	}

	@Override
	public boolean setWritable(boolean writable) {
		// s3.releaseCache(getInnerPath());
		// TODO
		return false;
	}

	public Array getAccessControlList() {
		try {
			return s3.getAccessControlList(bucketName, getObjectName());
		}
		catch (Exception e) {
			throw engine.getCastUtil().toPageRuntimeException(e);
		}
	}

	public void setAccessControlList(Object objAcl) {
		try {
			s3.setAccessControlList(bucketName, getObjectName(), objAcl);
		}
		catch (Exception e) {
			throw engine.getCastUtil().toPageRuntimeException(e);
		}
	}

	public void addAccessControlList(Object objAcl) {
		try {
			s3.addAccessControlList(bucketName, getObjectName(), objAcl);
		}
		catch (Exception e) {
			throw engine.getCastUtil().toPageRuntimeException(e);
		}
	}

	public Struct getMetaData() {
		try {
			return s3.getMetaData(bucketName, getObjectName());
		}
		catch (Exception e) {
			throw engine.getCastUtil().toPageRuntimeException(e);
		}
	}

	public void setMetaData(Struct metaData) {
		try {
			s3.setMetaData(bucketName, getObjectName(), metaData);
		}
		catch (Exception e) {
			throw engine.getCastUtil().toPageRuntimeException(e);
		}
	}

	public String getBucketName() {
		return bucketName;
	}

	public String getObjectName() {
		if (!engine.getStringUtil().isEmpty(objectName) && isDirectory()) {
			return objectName + "/";
		}
		return objectName;
	}

	public void setACL(String acl) {
		this.acl = S3.toACL(acl, null);
	}

	public void setLocation(String location) {
		this.location = S3.improveLocation(location);
	}

	public void setStorage(String storage) { // to not delete, exist because the core maybe call this with reflection
		setLocation(storage);
	}

	@Override
	public void copyFrom(Resource res, boolean append) throws IOException {
		copy(res, this, append);
	}

	@Override
	public void copyTo(Resource res, boolean append) throws IOException {
		copy(this, res, append);
	}

	private void copy(Resource from, Resource to, boolean append) throws IOException {
		if (from instanceof S3Resource && to instanceof S3Resource) {
			S3Resource f = (S3Resource) from;
			S3Resource t = (S3Resource) to;
			// whe have the same container
			if (f.s3.getAccessKeyId().equals(t.s3.getAccessKeyId()) && f.s3.getSecretAccessKey().equals(t.s3.getSecretAccessKey())) {
				s3.copyObject(f.bucketName, f.objectName, t.bucketName, t.objectName, null, null);
				return;
			}

		}
		super.copyTo(to, append);
	}

	@Override
	public void moveFile(Resource src, Resource trg) throws IOException {
		if (src instanceof S3Resource && trg instanceof S3Resource) {

			S3Resource s3Src = (S3Resource) src;
			S3Resource s3Trg = (S3Resource) trg;
			// we have the same container
			if (s3Trg.s3.getAccessKeyId().equals(s3Src.s3.getAccessKeyId()) && s3Trg.s3.getSecretAccessKey().equals(s3Src.s3.getSecretAccessKey())) {
				s3.moveObject(s3Src.bucketName, s3Src.objectName, s3Trg.bucketName, s3Trg.objectName, null, null);
				return;
			}
		}

		if (!trg.exists()) trg.createFile(false);
		Util.copy(src, trg);
		src.remove(false);
	}
}