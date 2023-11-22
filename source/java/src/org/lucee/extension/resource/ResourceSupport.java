/**
 *
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
 **/
package org.lucee.extension.resource;

import java.io.IOException;
import java.io.OutputStream;

import lucee.commons.io.res.Resource;
import lucee.commons.io.res.filter.ResourceFilter;
import lucee.commons.io.res.filter.ResourceNameFilter;
import lucee.loader.engine.CFMLEngine;

/**
 * Helper class to build resources
 */
public abstract class ResourceSupport implements Resource {

	protected final CFMLEngine engine;

	public ResourceSupport(CFMLEngine engine) {
		this.engine = engine;
	}

	@Override
	public void copyFrom(Resource res, boolean append) throws IOException {
		engine.getIOUtil().copy(res.getInputStream(), this.getOutputStream(append), true, true);
	}

	@Override
	public void copyTo(Resource res, boolean append) throws IOException {
		engine.getIOUtil().copy(this.getInputStream(), res.getOutputStream(append), true, true);
	}

	@Override
	public Resource getAbsoluteResource() {
		return this;
	}

	@Override
	public String getAbsolutePath() {
		return getPath();
	}

	@Override
	public OutputStream getOutputStream() throws IOException {
		return getOutputStream(false);
	}

	@Override
	public Resource getCanonicalResource() throws IOException {
		return this;
	}

	@Override
	public String getCanonicalPath() throws IOException {
		return getPath();
	}

	@Override
	public void moveTo(Resource dest) throws IOException {
		checkMoveToOK(this, dest);
		_moveTo(this, dest);
	}

	private void _moveTo(Resource src, Resource dest) throws IOException {

		if (src.isFile()) {
			moveFile(src, dest);
		}
		else {
			if (!dest.exists()) dest.createDirectory(false);
			Resource[] children = src.listResources();
			for (int i = 0; i < children.length; i++) {
				_moveTo(children[i], dest.getRealResource(children[i].getName()));
			}
			src.remove(false);
		}
		dest.setLastModified(System.currentTimeMillis());
	}

	public abstract void moveFile(Resource src, Resource dest) throws IOException;

	private static void checkMoveToOK(Resource source, Resource target) throws IOException {
		if (!source.exists()) {
			throw new IOException("can't move [" + source.getPath() + "] to [" + target.getPath() + "], source file does not exist");
		}
		if (source.isDirectory() && target.isFile()) throw new IOException("can't move [" + source.getPath() + "] directory to [" + target.getPath() + "], target is a file");
		if (source.isFile() && target.isDirectory()) throw new IOException("can't move [" + source.getPath() + "] file to [" + target.getPath() + "], target is a directory");
	}

	@Override
	public String[] list(ResourceFilter filter) {
		return list(null, filter);
	}

	@Override
	public String[] list(ResourceNameFilter filter) {
		return list(filter, null);
	}

	@Override
	public String[] list() {
		return list(null, null);
	}

	private String[] list(ResourceNameFilter nameilter, ResourceFilter filter) {
		Resource[] children = listResources(nameilter, filter);
		if (children == null) return null;
		String[] rtn = new String[children.length];
		for (int i = 0; i < children.length; i++) {
			rtn[i] = children[i].getName();
		}
		return rtn;
	}

	@Override
	public Resource[] listResources(ResourceNameFilter filter) {
		return listResources(filter, null);
	}

	@Override
	public Resource[] listResources(ResourceFilter filter) {
		return listResources(null, filter);
	}

	@Override
	public Resource[] listResources() {
		return listResources(null, null);
	}

	@Override
	public String getReal(String realpath) {
		return getRealResource(realpath).getPath();
	}

	@Override
	public boolean canRead() {
		return isReadable();
	}

	@Override
	public boolean canWrite() {
		return isWriteable();
	}

	@Override
	public boolean renameTo(Resource dest) {
		try {
			moveTo(dest);
			return true;
		}
		catch (IOException e) {
			return false;
		}

	}

	@Override
	public boolean createNewFile() {
		try {
			createFile(false);
			return true;
		}
		catch (IOException e) {
		}
		return false;
	}

	@Override
	public boolean mkdir() {
		try {
			createDirectory(false);
			return true;
		}
		catch (IOException e) {
		}
		return false;
	}

	@Override
	public boolean mkdirs() {
		try {
			createDirectory(true);
			return true;
		}
		catch (IOException e) {
			return false;
		}
	}

	@Override
	public boolean delete() {
		try {
			remove(false);
			return true;
		}
		catch (IOException e) {
		}
		return false;
	}

	@Override
	public boolean isArchive() {
		return getAttribute(Resource.ATTRIBUTE_ARCHIVE);
	}

	@Override
	public boolean isSystem() {
		return getAttribute(Resource.ATTRIBUTE_SYSTEM);
	}

	@Override
	public boolean isHidden() {
		return getAttribute(Resource.ATTRIBUTE_HIDDEN);
	}

	@Override
	public void setArchive(boolean value) throws IOException {
		setAttribute(ATTRIBUTE_ARCHIVE, value);
	}

	@Override
	public void setHidden(boolean value) throws IOException {
		setAttribute(ATTRIBUTE_HIDDEN, value);
	}

	@Override
	public boolean setReadOnly() {
		return setWritable(false);
	}

	@Override
	public void setSystem(boolean value) throws IOException {
		setAttribute(ATTRIBUTE_SYSTEM, value);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) return true;
		if (!(obj instanceof Resource)) return false;
		Resource other = (Resource) obj;

		if (getResourceProvider() != other.getResourceProvider()) return false;

		if (getResourceProvider().isCaseSensitive()) {
			if (getPath().equals(other.getPath())) return true;
			return getCanonicalPathEL(this).equals(getCanonicalPathEL(other));
		}
		if (getPath().equalsIgnoreCase(other.getPath())) return true;
		return getCanonicalPathEL(this).equalsIgnoreCase(getCanonicalPathEL(other));

	}

	@Override
	public String toString() {
		return getPath();
	}

	@Override
	public boolean getAttribute(short attribute) {
		return false;
	}

	@Override
	public void setAttribute(short attribute, boolean value) throws IOException {
		throw new IOException("the resource [" + getPath() + "] does not support attributes");
	}

	/**
	 * Returns the canonical form of this abstract pathname.
	 * 
	 * @param res file to get canoncial form from it
	 *
	 * @return The canonical pathname string denoting the same file or directory as this abstract
	 *         pathname
	 *
	 * @throws SecurityException If a required system property value cannot be accessed.
	 */
	private static String getCanonicalPathEL(Resource res) {
		try {
			return res.getCanonicalPath();
		}
		catch (IOException e) {
			return res.toString();
		}
	}

	public abstract Resource[] listResources(ResourceNameFilter nameFilter, ResourceFilter filter);
}