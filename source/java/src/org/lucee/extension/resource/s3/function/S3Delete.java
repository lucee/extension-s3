package org.lucee.extension.resource.s3.function;

import java.util.ArrayList;
import java.util.List;

import org.lucee.extension.resource.s3.S3;
import org.lucee.extension.resource.s3.S3DeleteResult;
import org.lucee.extension.resource.s3.S3Util;

import com.amazonaws.services.s3.model.DeleteObjectsRequest.KeyVersion;

import lucee.loader.engine.CFMLEngine;
import lucee.loader.engine.CFMLEngineFactory;
import lucee.loader.util.Util;
import lucee.runtime.PageContext;
import lucee.runtime.exp.PageException;
import lucee.runtime.type.Array;
import lucee.runtime.type.Collection;
import lucee.runtime.type.Struct;
import lucee.runtime.util.Cast;

public class S3Delete extends S3Function {

	private static final long serialVersionUID = -8478080686638374027L;

	public static Object call(PageContext pc, String bucketName, String objectName, boolean force, String accessKeyId, String secretAccessKey, String host, double timeout)
			throws PageException {
		return call(pc, bucketName, objectName, force, accessKeyId, secretAccessKey, host, timeout, false);
	}

	public static Object call(PageContext pc, String bucketName, String objectName, boolean force, String accessKeyId, String secretAccessKey, String host, double timeout,
			boolean ignoreMissing) throws PageException {

		CFMLEngine eng = CFMLEngineFactory.getInstance();
		// for backward compatibility, when host was not existing
		if (eng.getDecisionUtil().isNumber(host)) {
			timeout = eng.getCastUtil().toDoubleValue(host);
			host = null;
		}
		try {
			S3 s3 = S3.getInstance(toS3Properties(pc, accessKeyId, secretAccessKey, host), toTimeout(timeout), pc.getConfig());
			if (Util.isEmpty(objectName, true)) s3.delete(bucketName, force, ignoreMissing);
			else s3.delete(bucketName, objectName, force, ignoreMissing);
			return null;
		}
		catch (Exception e) {
			throw S3Util.toPageException(e);
		}
	}

	/**
	 * Deletes the objects of an array in as few requests as possible (1000 objects per request).
	 * 
	 * @param objectNames array of object names (strings) or structs with the keys "key" and optionally
	 *            "versionId" (only that version is deleted)
	 * @param throwOnError if true an exception is thrown after all objects were processed when at least
	 *            one object could not be deleted, otherwise the failures are part of the result
	 * @return struct with the keys "deleted" (array) and "errors" (array of structs with the keys key,
	 *         versionId, code and message)
	 */
	public static Object call(PageContext pc, String bucketName, Object objectNames, String accessKeyId, String secretAccessKey, String host, double timeout,
			boolean throwOnError) throws PageException {
		CFMLEngine eng = CFMLEngineFactory.getInstance();
		List<KeyVersion> keys = toKeyVersions(eng, objectNames);
		S3DeleteResult result;
		try {
			S3 s3 = S3.getInstance(toS3Properties(pc, accessKeyId, secretAccessKey, host), toTimeout(timeout), pc.getConfig());
			result = s3.deleteObjects(bucketName, keys);
		}
		catch (Exception e) {
			throw S3Util.toPageException(e);
		}

		if (throwOnError && !result.getErrors().isEmpty()) {
			StringBuilder sb = new StringBuilder();
			int count = 0;
			for (S3DeleteResult.Failure f: result.getErrors()) {
				if (count++ == 10) {
					sb.append(", ...");
					break;
				}
				if (sb.length() > 0) sb.append(", ");
				sb.append(f.getKey());
				if (f.getVersionId() != null) sb.append(" (version ").append(f.getVersionId()).append(")");
				sb.append(": ").append(f.getCode()).append(" - ").append(f.getMessage());
			}
			throw eng.getExceptionUtil().createApplicationException(
					"could not delete " + result.getErrors().size() + " of " + (result.getDeleted().size() + result.getErrors().size()) + " objects in bucket [" + bucketName
							+ "]: " + sb);
		}
		return toStruct(eng, result);
	}

	private static List<KeyVersion> toKeyVersions(CFMLEngine eng, Object objectNames) throws PageException {
		Cast cast = eng.getCastUtil();
		Array arr = cast.toArray(objectNames);
		List<KeyVersion> keys = new ArrayList<>();
		Collection.Key keyKey = eng.getCreationUtil().createKey("key");
		Collection.Key versionIdKey = eng.getCreationUtil().createKey("versionId");
		int len = arr.size();
		for (int i = 1; i <= len; i++) {
			Object el = arr.getE(i);
			String key, versionId = null;
			if (eng.getDecisionUtil().isStruct(el)) {
				Struct sct = cast.toStruct(el);
				key = cast.toString(sct.get(keyKey, null), null);
				if (Util.isEmpty(key, true)) throw eng.getExceptionUtil().createApplicationException("the struct at position [" + i + "] of the argument [objectName] needs the key [key]");
				versionId = cast.toString(sct.get(versionIdKey, null), null);
			}
			else {
				key = cast.toString(el, null);
				if (Util.isEmpty(key, true))
					throw eng.getExceptionUtil().createApplicationException("the entry at position [" + i + "] of the argument [objectName] is not a valid object name, use a string or a struct with the keys [key] and [versionId]");
			}
			keys.add(new KeyVersion(key, versionId));
		}
		return keys;
	}

	private static Struct toStruct(CFMLEngine eng, S3DeleteResult result) {
		Struct sct = eng.getCreationUtil().createStruct();

		Array deleted = eng.getCreationUtil().createArray();
		for (KeyVersion kv: result.getDeleted()) {
			if (kv.getVersion() == null) deleted.appendEL(kv.getKey());
			else {
				Struct item = eng.getCreationUtil().createStruct();
				item.setEL("key", kv.getKey());
				item.setEL("versionId", kv.getVersion());
				deleted.appendEL(item);
			}
		}
		sct.setEL("deleted", deleted);

		Array errors = eng.getCreationUtil().createArray();
		for (S3DeleteResult.Failure f: result.getErrors()) {
			Struct item = eng.getCreationUtil().createStruct();
			item.setEL("key", f.getKey());
			item.setEL("versionId", f.getVersionId() == null ? "" : f.getVersionId());
			item.setEL("code", f.getCode());
			item.setEL("message", f.getMessage());
			errors.appendEL(item);
		}
		sct.setEL("errors", errors);
		return sct;
	}

	@Override
	public Object invoke(PageContext pc, Object[] args) throws PageException {
		CFMLEngine engine = CFMLEngineFactory.getInstance();
		Cast cast = engine.getCastUtil();
		// an array of objects: delete all of them in one call
		if (args.length >= 2 && engine.getDecisionUtil().isArray(args[1])) {
			if (args.length > 9) throw engine.getExceptionUtil().createFunctionException(pc, "S3Delete", 1, 9, args.length);
			// force (3) and ignoreMissing (8) have no meaning for single objects, a missing object is deleted by definition
			return call(pc, cast.toString(args[0]), args[1], args.length > 3 ? cast.toString(args[3], null) : null, args.length > 4 ? cast.toString(args[4], null) : null,
					args.length > 5 ? cast.toString(args[5], null) : null, args.length > 6 && args[6] != null ? cast.toDoubleValue(args[6]) : 0,
					args.length > 8 && args[8] != null && cast.toBooleanValue(args[8]));
		}
		if (args.length == 9) return call(pc, cast.toString(args[0]), cast.toString(args[1]), cast.toBooleanValue(args[2]), cast.toString(args[3]), cast.toString(args[4]),
				cast.toString(args[5]), cast.toDoubleValue(args[6]), args[7] != null && cast.toBooleanValue(args[7]));
		if (args.length == 8) return call(pc, cast.toString(args[0]), cast.toString(args[1]), cast.toBooleanValue(args[2]), cast.toString(args[3]), cast.toString(args[4]),
				cast.toString(args[5]), cast.toDoubleValue(args[6]), args[7] != null && cast.toBooleanValue(args[7]));
		if (args.length == 7) return call(pc, cast.toString(args[0]), cast.toString(args[1]), cast.toBooleanValue(args[2]), cast.toString(args[3]), cast.toString(args[4]),
				cast.toString(args[5]), cast.toDoubleValue(args[6]));
		if (args.length == 6) return call(pc, cast.toString(args[0]), cast.toString(args[1]), cast.toBooleanValue(args[2]), cast.toString(args[3]), cast.toString(args[4]),
				cast.toString(args[5]), 0);
		if (args.length == 5)
			return call(pc, cast.toString(args[0]), cast.toString(args[1]), cast.toBooleanValue(args[2]), cast.toString(args[3]), cast.toString(args[4]), null, 0);
		if (args.length == 4) return call(pc, cast.toString(args[0]), cast.toString(args[1]), cast.toBooleanValue(args[2]), cast.toString(args[3]), null, null, 0);
		if (args.length == 3) return call(pc, cast.toString(args[0]), cast.toString(args[1]), cast.toBooleanValue(args[2]), null, null, null, 0);
		if (args.length == 2) return call(pc, cast.toString(args[0]), cast.toString(args[1]), true, null, null, null, 0);
		if (args.length == 1) return call(pc, cast.toString(args[0]), null, true, null, null, null, 0);

		throw engine.getExceptionUtil().createFunctionException(pc, "S3Delete", 1, 9, args.length);
	}
}