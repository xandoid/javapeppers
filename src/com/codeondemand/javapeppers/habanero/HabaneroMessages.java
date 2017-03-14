package com.codeondemand.javapeppers.habanero;

import java.util.MissingResourceException;
import java.util.ResourceBundle;

public class HabaneroMessages {
	private static final String BUNDLE_NAME = "com.javapeppers.habanero.messages"; //$NON-NLS-1$

	private static final ResourceBundle RESOURCE_BUNDLE = ResourceBundle
			.getBundle(BUNDLE_NAME);

	private HabaneroMessages() {
	}

	public static String getString(String key) {
		try {
			return RESOURCE_BUNDLE.getString(key);
		} catch (MissingResourceException e) {
			return '!' + key + '!';
		}
	}
}
