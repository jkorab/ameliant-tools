package com.ameliant.tools.kafka.performance.util;

import org.apache.commons.lang.RandomStringUtils;

import java.io.File;
import static java.io.File.separator;

/**
 * @author jkorab
 */
public class DirectoryUtils {

    private static String ioTempDir = System.getProperty("java.io.tmpdir");

    private static String tempDirString(String suffix) {
        return ioTempDir + separator + suffix;
    }

    public static String perTest(String directory) {
        return RandomStringUtils.randomAlphanumeric(8) + "-" + directory;
    }

    public static File tempDir(String suffix) {
        return new File(tempDirString(suffix));
    }

    public static File locateDirectory(String path) {
        if (path == null) {
            return null;
        }
        File dir = new File(path);
        return (dir.exists() && dir.isDirectory()) ? dir
                : locateDirectory(path.substring(path.indexOf('/')));
    }

}
