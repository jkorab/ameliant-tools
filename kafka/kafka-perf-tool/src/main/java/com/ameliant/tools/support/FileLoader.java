package com.ameliant.tools.support;

import org.apache.commons.lang.Validate;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * Utility class for reading in files.
 * @author jkorab
 */
public class FileLoader {

    public String loadFileAsString(String fileName) {
        Validate.notEmpty(fileName, "fileName is empty");
        try {
            FileInputStream fileInputStream = new FileInputStream(fileName);

            StringBuffer sb = new StringBuffer();
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(fileInputStream))) {
                while (true) {
                    String line = reader.readLine();
                    if (line == null) {
                        break;
                    } else {
                        if (sb.length() > 0) {
                            sb.append(System.lineSeparator());
                        }
                        sb.append(line);
                    }
                }
            }
            return sb.toString();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
