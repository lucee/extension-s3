package org.lucee.extension.resource.s3.function;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Method;
import java.nio.charset.Charset;
import java.util.Arrays;

import org.lucee.extension.resource.s3.S3;

import com.amazonaws.services.s3.model.S3Object;

import lucee.commons.io.res.Resource;
import lucee.commons.lang.types.RefInteger;
import lucee.loader.engine.CFMLEngine;
import lucee.loader.engine.CFMLEngineFactory;
import lucee.runtime.Component;
import lucee.runtime.PageContext;
import lucee.runtime.exp.PageException;
import lucee.runtime.type.Collection.Key;
import lucee.runtime.type.FunctionArgument;
import lucee.runtime.type.UDF;
import lucee.runtime.util.Cast;

public class S3Download extends S3Function {

	private static final long serialVersionUID = 8926919958105910628L;

	public static final short TYPE_ANY = 0;
	public static final short TYPE_BOOLEAN = 2;
	public static final short TYPE_STRING = 7;

	public static final short MODE_LINE = 1;
	public static final short MODE_BINARY = 2;
	public static final short MODE_STRING = 4;

	@Override
	public Object invoke(PageContext pc, Object[] args) throws PageException {
		CFMLEngine eng = CFMLEngineFactory.getInstance();
		Cast cast = eng.getCastUtil();
		if (args.length > 8 || args.length < 2) throw eng.getExceptionUtil().createFunctionException(pc, "S3Download", 2, 8, args.length);

		// required
		String bucketName = cast.toString(args[0]);
		String objectName = cast.toString(args[1]);

		// optional
		Object target = args.length > 2 && args[2] != null ? args[2] : null;
		Charset charset = args.length > 3 && args[3] != null ? cast.toCharset(cast.toString(args[3])) : null;
		String accessKeyId = args.length > 4 && args[4] != null ? cast.toString(args[4]) : null;
		String secretAccessKey = args.length > 5 && args[5] != null ? cast.toString(args[5]) : null;
		String host = args.length > 6 && args[6] != null ? cast.toString(args[6]) : null;
		double timeout = args.length > 7 && !isEmpty(args[7]) ? cast.toDoubleValue(args[7]) : 0;

		// validate
		UDF targetUDF = null;
		Component targetCFC = null;
		Resource targetRes = null;
		RefInteger mode = eng.getCreationUtil().createRefInteger(0);
		RefInteger blockSize = eng.getCreationUtil().createRefInteger(0);
		Key INVOKE = eng.getCastUtil().toKey("invoke");
		Key BEFORE = eng.getCastUtil().toKey("before");
		Key AFTER = eng.getCastUtil().toKey("after");

		if (target != null) {
			if (target instanceof UDF) {
				targetUDF = (UDF) target;
				validateInvoke(pc, targetUDF, mode, blockSize, false);
			}
			else if (target instanceof Component) {
				targetCFC = (Component) target;
				Component csa = toComponentSpecificAccess(Component.ACCESS_PRIVATE, targetCFC);
				boolean hasBefore = toFunction(csa.get(BEFORE, null), null) != null;
				boolean hasAfter = toFunction(csa.get(AFTER, null), null) != null;
				UDF invoke = toFunction(csa.get(INVOKE), null);
				if (invoke == null) throw eng.getExceptionUtil().createFunctionException(pc, "S3Download", 2, "component",
						"the listener component does not contain a instance function with name [invoke] that is required", null);
				validateInvoke(pc, invoke, mode, blockSize, false);
			}
			else if ((targetRes = S3Write.toResource(pc, target, false, null)) == null) {
				// can also be a charset defintion
				Charset tmp = charset == null ? eng.getCastUtil().toCharset(eng.getCastUtil().toString(target, null), null) : null;
				if (tmp == null) throw eng.getExceptionUtil().createFunctionException(pc, "S3Download", 3, "target",
						"the value of the argument needs to be a closure/function, a file name or not defined at all", "");
				charset = tmp;
			}
		}

		// create S3 Instance
		try {
			S3 s3 = S3.getInstance(toS3Properties(pc, accessKeyId, secretAccessKey, host), toTimeout(timeout), pc.getConfig());
			S3Object obj = s3.getData(bucketName, objectName);
			Cast caster = eng.getCastUtil();
			// stream to UDF
			boolean isUDF;
			if ((isUDF = (targetUDF != null)) || targetCFC != null) {

				// LINE
				if (MODE_LINE == mode.toInt()) {
					BufferedReader reader = null;
					try {
						if (charset == null) charset = pc.getConfig().getResourceCharset();
						reader = new BufferedReader(new InputStreamReader(obj.getObjectContent(), charset));
						String line;
						if (!isUDF) {
							targetCFC.call(pc, BEFORE, new Object[] {});
						}
						while ((line = reader.readLine()) != null) {
							if (isUDF) {
								if (!caster.toBooleanValue(targetUDF.call(pc, new Object[] { line }, true))) return null;
							}
							else {
								if (!caster.toBooleanValue(targetCFC.call(pc, INVOKE, new Object[] { line }))) {
									targetCFC.call(pc, AFTER, new Object[] {});
									return null;
								}
							}
						}
						if (!isUDF) {
							targetCFC.call(pc, AFTER, new Object[] {});
						}
						return null;
					}
					finally {
						eng.getIOUtil().closeSilent(reader);
					}
				}
				// STRING
				else if (MODE_STRING == mode.toInt()) {
					BufferedReader reader = null;
					char[] buffer = new char[blockSize.toInt()];
					try {
						if (!isUDF) {
							targetCFC.call(pc, BEFORE, new Object[] {});
						}
						if (charset == null) charset = pc.getConfig().getResourceCharset();
						reader = new BufferedReader(new InputStreamReader(obj.getObjectContent(), charset));
						int numCharsRead;
						while ((numCharsRead = reader.read(buffer, 0, buffer.length)) != -1) {
							String block = new String(buffer, 0, numCharsRead);
							if (isUDF) {
								if (!caster.toBooleanValue(targetUDF.call(pc, new Object[] { block }, true))) return null;
							}
							else {
								if (!caster.toBooleanValue(targetCFC.call(pc, INVOKE, new Object[] { block }))) {
									targetCFC.call(pc, AFTER, new Object[] {});
									return null;
								}
							}
						}
						if (!isUDF) {
							targetCFC.call(pc, AFTER, new Object[] {});
						}
						return null;
					}
					finally {
						eng.getIOUtil().closeSilent(reader);
					}
				}
				// BINARY
				else {
					InputStream input = null;
					byte[] buffer = new byte[blockSize.toInt()];
					byte[] arg;
					try {
						if (!isUDF) {
							targetCFC.call(pc, BEFORE, new Object[] {});
						}
						input = new BufferedInputStream(obj.getObjectContent());
						int bytesRead;
						while ((bytesRead = input.read(buffer)) != -1) {

							if (bytesRead == buffer.length) arg = buffer;
							else arg = Arrays.copyOf(buffer, bytesRead);
							if (isUDF) {
								if (!caster.toBooleanValue(targetUDF.call(pc, new Object[] { arg }, true))) return null;
							}
							else {
								if (!caster.toBooleanValue(targetCFC.call(pc, INVOKE, new Object[] { arg }))) {
									targetCFC.call(pc, AFTER, new Object[] {});
									return null;
								}
							}
						}
						if (!isUDF) {
							targetCFC.call(pc, AFTER, new Object[] {});
						}
						return null;
					}
					finally {
						eng.getIOUtil().closeSilent(input);
					}
				}

			}
			// store to file
			else if (targetRes != null) {
				eng.getIOUtil().copy(obj.getObjectContent(), targetRes, true);
				return null;
			}
			// return the value
			else {
				if (charset == null) {
					// TODO get mimetype info from S3 and decide based on this if we provide a byte array or a string
					ByteArrayOutputStream baos = new ByteArrayOutputStream();
					eng.getIOUtil().copy(obj.getObjectContent(), baos, true, true);
					return baos.toByteArray();
				}
				return eng.getIOUtil().toString(obj.getObjectContent(), charset);
			}
		}
		catch (Exception e) {
			throw eng.getCastUtil().toPageException(e);
		}
	}

	public static UDF toFunction(Object obj, UDF defaultValue) {
		if (obj instanceof UDF) return (UDF) obj;
		return defaultValue;
	}

	private void validateInvoke(PageContext pc, UDF targetUDF, RefInteger mode, RefInteger blockSize, boolean member) throws PageException {

		String nameDesc = member ? "function [invoke] of the listener component" : "closure/function";

		CFMLEngine eng = CFMLEngineFactory.getInstance();
		// function return type
		if (!(targetUDF.getReturnType() == TYPE_ANY || targetUDF.getReturnType() == TYPE_BOOLEAN))
			throw eng.getExceptionUtil().createFunctionException(pc, "S3Download", 3, "target", "the " + nameDesc + " must have the return type boolean.", "");

		// function invoke arguments
		FunctionArgument[] udfArgs = targetUDF.getFunctionArguments();
		if (udfArgs.length < 1 || udfArgs.length > 1) throw eng.getExceptionUtil().createFunctionException(pc, "S3Download", 3, "target",
				"you need to define an argument for the " + nameDesc + " passed in following this pattern (string line|binary{number}|string{number})", "");

		FunctionArgument arg = udfArgs[0];
		if (!(arg.getType() == TYPE_ANY || arg.getType() == TYPE_STRING)) throw eng.getExceptionUtil().createFunctionException(pc, "S3Download", 3, "target",
				"the first argument of the  " + nameDesc + " need to be defined as a string or no defintion at all", "");

		String name = (arg.getName().getString() + "").toLowerCase().trim();
		if ("line".equals(name)) {
			mode.setValue(MODE_LINE);
		}
		else if (name.startsWith("binary")) {
			mode.setValue(MODE_BINARY);
			blockSize.setValue(eng.getCastUtil().toIntValue(name.substring(6)));
			if (blockSize.toInt() <= 0) throw eng.getExceptionUtil().createFunctionException(pc, "S3Download", 3, "target",
					"invalid block size defintion with the argument [binary{Number}], blocksize need to be a positive number, so the argument name should for example look like this [binary1000] to get block size 1000",
					"");
		}
		else if (name.startsWith("string")) {
			mode.setValue(MODE_STRING);
			blockSize.setValue(eng.getCastUtil().toIntValue(name.substring(6)));
			if (blockSize.toInt() <= 0) throw eng.getExceptionUtil().createFunctionException(pc, "S3Download", 3, "target",
					"invalid block size defintion with the argument [string{Number}], blocksize need to be a positive number, so the argument name should for example look like this [string1000] to get block size 1000",
					"");
		}
		else {
			throw eng.getExceptionUtil().createFunctionException(pc, "S3Download", 3, "target", "the first argument of the " + nameDesc
					+ " need to be define an argument where the name does follow one of this patterns [line, binary(Number), string(Number) ]", "");
		}
	}

	public static Component toComponentSpecificAccess(int access, Component component) throws PageException {
		try {
			Class<?> clazz = CFMLEngineFactory.getInstance().getClassUtil().loadClass("lucee.runtime.ComponentSpecificAccess");
			Method m = clazz.getMethod("toComponentSpecificAccess", new Class[] { int.class, Component.class });
			return (Component) m.invoke(null, new Object[] { access, component });
		}
		catch (Exception e) {
			throw CFMLEngineFactory.getInstance().getCastUtil().toPageException(e);
		}
	}
}