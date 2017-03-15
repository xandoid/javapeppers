/**
 *
 */
package com.codeondemand.javapeppers.habanero.util.misc;

/**
 * A simple application that will provide a fairly simple encryption
 * of a string (typically a password string) that will be suitable for
 * use by other components in the javapeppers libraries.
 *
 * @author Gary Anderson
 */
public class DecryptPWDString {

    /**
     * @param args
     */
    public static void main(String[] args) {
        for (String arg : args) {
            System.out.print(arg + ":");
            System.out.println(MiscUtil.decodeB64String(arg));
        }
    }

}
