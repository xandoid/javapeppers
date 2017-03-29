package com.codeondemand.javapeppers.aleppo.reader;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;

import java.io.File;
import java.util.Collection;
import java.util.Iterator;
import java.util.StringTokenizer;

public class FileTreeReader extends SourceReader {

    @Override
    public boolean close() {
        return true;
    }

    @Override
    public boolean reset() {
        doInitialization();
        return true;
    }

    @Override
    public Object read() {

        File retval = null;
        if (!initialized) {
            initialized = doInitialization();
        }
        if (ifiles != null && ifiles.hasNext()) {
            File foo = (File) ifiles.next();
            if (foo.isFile() && listFiles) {
                retval = foo.getAbsoluteFile();
            } else {
                if (foo.isDirectory() && listDirs) {
                    retval = foo.getAbsoluteFile();
                }
            }
        }
        return retval;
    }

    @Override
    public boolean doInitialization() {

        if (pmap.containsKey("root")) {
            root = (String) pmap.get("root");
        }
        if (pmap.containsKey("extensions")) {
            String temp = (String) pmap.get("extensions");
            StringTokenizer stok = new StringTokenizer(temp, "|");
            extensions = new String[stok.countTokens()];
            int i = 0;
            while (stok.hasMoreTokens()) {
                extensions[i++] = stok.nextToken();
            }
        }
        if (pmap.containsKey("list_dirs")) {
            listDirs = Boolean.parseBoolean((String) pmap.get("list_dirs"));
        }
        if (pmap.containsKey("list_files")) {
            listFiles = Boolean.parseBoolean((String) pmap.get("list_files"));
        }

        if (pmap.containsKey("recurse")) {
            recurse = Boolean.parseBoolean((String) pmap.get("recurse"));
        }
        logger.debug(root + "|" + listDirs + "|" + listFiles + "|" + recurse + "|" + extensions[0]);
        File r = new File(root);
        if (r.isDirectory()) {
            ifiles = FileUtils.listFiles(r, extensions, recurse).iterator();

        } else {
            logger.error("Cannot list files.");
        }

        return true;
    }

    private boolean initialized = false;
    private String root = null;
    private String[] extensions = null;
    private boolean recurse = false;
    Collection<File> f = null;
    Iterator ifiles = null;
    private boolean listDirs = false;
    private boolean listFiles = true;
    private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("FileTreeReader");

}
