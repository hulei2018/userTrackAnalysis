package com.usertrack.conf;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ConfigurationManager {
    private static Properties prop=new Properties();
    static {
        InputStream in = ConfigurationManager.class.getClassLoader().getResourceAsStream("usertrack.properties");
        try {
            prop.load(in);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     *
     * @param key
     * @return
     */
    public static String getProperty(String key){
        return prop.getProperty(key);
    }

    /**
     * @param key String
     * @return Integer
     */

    public static int getInteger(String key){
        String value=getProperty(key);
        return Integer.parseInt(value);
    }

}
