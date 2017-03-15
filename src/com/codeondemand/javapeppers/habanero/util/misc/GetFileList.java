package com.codeondemand.javapeppers.habanero.util.misc;

import org.apache.logging.log4j.LogManager;

import java.io.File;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

/**
 * The GetFileList is just a convenience class to allow a simple collection of
 * files and/or directories to be returned in an iterator. Note that this class
 * does not have a provision for returning the undelying structure of the file
 * collection. The iterator will return a list sorted with the default order.
 * <p>
 * If you do not specify a file pattern you will receive a list of unfiltered
 * files and/or directories for the starting point you specified.
 * <p>
 * If you do not specify anything, the default is to return only files from the
 * current directory.
 *
 * @author gfa
 */

public class GetFileList implements FilenameFilter {

    /**
     * Basic constructor.
     */
    public GetFileList() {

    }

    /**
     * Constructor allowing you to specify what directory you want to search and
     * the pattern you want to use.
     *
     * @param dir     The directory location you want search. If this is null, then
     *                it will default to your current directory.
     * @param pattern The pattern (only supports 'starts with' pattern currently,
     *                not wildcards and/or 'contains'.
     */
    public GetFileList(String dir, String pattern) {
        if (file_pattern != null) {
            file_pattern = pattern;
        }
        if (dir != null) {
            file_dir = dir;
        }
    }

    // Internally as part of FilenameFilter interface
    public boolean accept(File dir, String name) {
        return CheckPattern(name);
    }

    private boolean CheckPattern(String name) {
        boolean retval = false;
        if (file_pattern == null || name.startsWith(file_pattern)) {
            File bar = new File(file_dir + File.separator + name);

            // Return files and/or directories depending on the flag settings.
            if ((bar.isFile() && getFiles) || (bar.isDirectory() && getDirectories)) {
                files.add(file_dir + File.separator + name);
            }
            retval = true;
        }
        return retval;
    }

    public Iterator<String> getList() {
        files.clear();
        File foo = new File(file_dir);
        if (foo != null && foo.isDirectory()) {
            foo.listFiles(this);
        }
        Collections.sort(files);
        return files.iterator();
    }

    /**
     * Turns the flag controlling returning directory entries on/off
     *
     * @param getDirectories if true, return directories as well as files.
     */
    public void setGetDirectories(boolean getDirectories) {
        this.getDirectories = getDirectories;
    }

    /**
     * Turn the flag on controlling returning file entries on/off
     *
     * @param getFiles If true, then return files, otherwise not.
     */
    public void setGetFiles(boolean getFiles) {
        this.getFiles = getFiles;
    }

    /**
     * Sets the directory you want to check. If set to null, then the current
     * directory is checked.
     *
     * @param dir Directory to check. If null, will be set to '.'.
     */
    public void setFile_dir(String dir) {
        if (file_dir == null) {
            file_dir = ".";
        } else {
            File foo = new File(dir);
            if (foo != null && foo.isDirectory()) {
                file_dir = dir;
            } else {
                logger.error("Invalid directory entry specified");
                file_dir = ".";
            }
        }
    }

    /**
     * Sets the file pattern to use. Only currently supports a 'startswith' type
     * comparison.
     *
     * @param file_pattern Comparison pattern to use. If null, then no filtering.
     */
    public void setFile_pattern(String file_pattern) {
        this.file_pattern = file_pattern;
    }

    public static void main(String[] args) {
        GetFileList gfl = new GetFileList("e:/dev/workspace", "*.java");
        gfl.setGetDirectories(true);
        gfl.getList();

        @SuppressWarnings("rawtypes") Iterator foo = gfl.files.iterator();
        while (foo.hasNext()) {
            String s = foo.next().toString();
            File bar = new File(s);
            if (bar.isFile()) {
                System.out.println(s + ": " + bar.length());
            } else {
                System.out.println("directory: " + s);
            }
        }
    }

    private boolean getFiles = true;
    private boolean getDirectories = false;

    private String file_dir = ".";
    private String file_pattern = null;

    private ArrayList<String> files = new ArrayList<String>();
    private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("GetFileList");

}
