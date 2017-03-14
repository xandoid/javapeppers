package com.codeondemand.javapeppers.ancho.io;

import java.util.Iterator;

import com.codeondemand.javapeppers.habanero.util.misc.GetFileList;

public class TestFileList {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String pattern = null;
		String dir = null;
		if (args.length > 0) {
			dir = args[0];
		}
		if (args.length > 1) {
			pattern = args[1];
		}
		GetFileList foo = new GetFileList(dir, pattern);
		foo.setGetDirectories(true);
		foo.setGetFiles(false);
		System.out.println("\nRun test getting directories only.");
		Iterator<String> it = foo.getList();
		while (it.hasNext()) {
			System.out.println(it.next());
		}
		foo.setGetDirectories(false);
		foo.setGetFiles(true);
		System.out.println("\nRun test getting files only");
		it = foo.getList();
		while (it.hasNext()) {
			System.out.println(it.next());
		}

		foo.setGetDirectories(true);
		foo.setGetFiles(true);
		System.out.println("\nRun test getting directories and files.");
		it = foo.getList();
		while (it.hasNext()) {
			System.out.println(it.next());
		}

	}

}
