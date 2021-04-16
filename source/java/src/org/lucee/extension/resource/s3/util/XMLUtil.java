package org.lucee.extension.resource.s3.util;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.FactoryConfigurationError;

import lucee.loader.util.Util;

public class XMLUtil {

	public static void validateDocumentBuilderFactory() {
		try {
			DocumentBuilderFactory.newInstance();
		}
		catch (FactoryConfigurationError err) {
			String className = newDocumentBuilderFactoryClassName();
			if (!Util.isEmpty(className)) {
				System.setProperty("javax.xml.parsers.DocumentBuilderFactory", className);
				DocumentBuilderFactory.newInstance();
			}
			else throw err;
		}

	}

	private static Class<DocumentBuilderFactory> newDocumentBuilderFactoryClass() {
		Class<?> clazz = null;
		try {
			clazz = Class.forName("org.apache.xerces.internal.jaxp.DocumentBuilderFactoryImpl");
		}
		catch (Exception e) {
			try {
				clazz = Class.forName("com.sun.org.apache.xerces.internal.jaxp.DocumentBuilderFactoryImpl");
			}
			catch (Exception ee) {
			}
		}

		return (Class<DocumentBuilderFactory>) clazz;
	}

	private static String newDocumentBuilderFactoryClassName() {
		Class<DocumentBuilderFactory> clazz = newDocumentBuilderFactoryClass();
		if (clazz == null) return null;
		return clazz.getName();
	}
}
