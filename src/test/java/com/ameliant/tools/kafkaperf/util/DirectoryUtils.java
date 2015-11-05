package com.ameliant.tools.kafkaperf.util;

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

}
