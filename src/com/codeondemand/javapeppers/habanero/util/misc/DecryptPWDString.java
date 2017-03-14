/**
 * 
 */
package com.codeondemand.javapeppers.habanero.util.misc;

import com.codeondemand.javapeppers.habanero.util.misc.MiscUtil;
/**
 * A simple application that will provide a fairly simple encryption
 * of a string (typically a password string) that will be suitable for
 * use by other components in the javapeppers libraries.
 *  
 * @author Gary Anderson
 *
 */
public class DecryptPWDString {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		for( int i = 0 ; i < args.length ; i++){
			System.out.print( args[i]+":");
			System.out.println( MiscUtil.decodeB64String(args[i]));
		}
	}

}
