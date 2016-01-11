package com.ameliant.tools.kafkaperf.util;

/**
 * @author jkorab
 */
public class PayloadDetector {

    public static boolean isYaml(String configType) {
        return (configType.equals("yml") || configType.equals("yaml"));
    }

    public static boolean isYamlFile(String fileName) {
        assert (fileName != null);
        return isYaml(fileName.substring(fileName.lastIndexOf(".") + 1));
    }

}
