/**
 * 
 */
package com.codeondemand.javapeppers.aleppo;

import java.util.MissingResourceException;
import java.util.ResourceBundle;

public class AleppoMessages {
	private static final String BUNDLE_NAME = "com.codeondemand.javapeppers.aleppo.aleppo_messages"; //$NON-NLS-1$

	private static final ResourceBundle RESOURCE_BUNDLE = ResourceBundle
			.getBundle(BUNDLE_NAME);

	private AleppoMessages() {
	}

	public static String getString(String key) {
		try {
			return RESOURCE_BUNDLE.getString(key);
		} catch (MissingResourceException e) {
			return '!' + key + '!';
		}
	}
}
