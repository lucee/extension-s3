package lucee.commons.io.res;

import java.io.IOException;

import lucee.commons.io.res.filter.ResourceFilter;

/**
 * COMPILE TIME STUB, NEVER PART OF THE EXTENSION (it is excluded from the jar, see pom.xml).
 * <p>
 * This is the interface Lucee itself provides since the version that introduced it (LDEV-4003), it
 * only exists here because the Lucee version this extension is compiled against (see the
 * dependency "lucee" in pom.xml) does not have it yet. At runtime the real interface of the Lucee
 * running the extension is used, or, with an older Lucee, the classes that need it are never loaded
 * (see S3ResourceFactory).
 * <p>
 * Remove this stub as soon as the Lucee dependency in pom.xml is a version that contains
 * lucee.commons.io.res.ResourceCopyMove, and the exclusions in pom.xml with it.
 */
public interface ResourceCopyMove {

	public boolean copyDirectory(Resource target, ResourceFilter filter) throws IOException;

	public boolean moveDirectory(Resource target) throws IOException;
}
