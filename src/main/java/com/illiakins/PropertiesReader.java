package com.illiakins;

import com.netflix.config.ConfigurationManager;
import com.netflix.config.DynamicPropertyFactory;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Created by Illiak on 2/1/2017.
 */
public class PropertiesReader {
    private static final Logger LOG = Logger.getLogger(PropertiesReader.class);
    private static DynamicPropertyFactory dynamicPropertyFactory;

    static {
        String env = System.getProperty("archaius.deployment.environment");
        LOG.debug("validating environment");
        if(env == null || env.isEmpty() || env.equalsIgnoreCase("Undefined")){
            LOG.debug("environment is not defined");
            env = "config";
        }else {
            LOG.debug("environment is defined as " + env);
        }
        try {
            ConfigurationManager.loadCascadedPropertiesFromResources(env);
        } catch (IOException e) {
            LOG.error("UNABLE TO SET DEFAULT CONFIG FILE", e);
        }
        dynamicPropertyFactory = DynamicPropertyFactory.getInstance();
    }

    public static String getStringSetting(String settingName){
        return dynamicPropertyFactory.getStringProperty(settingName, "").get();
    }

    public static int getIntSetting(String settingName){
        return dynamicPropertyFactory.getIntProperty(settingName, -1).get();
    }

    public static boolean getBooleanSetting(String settingName){
        return dynamicPropertyFactory.getBooleanProperty(settingName, false).get();
    }

    public static String[] getArraySetting(String settingName){
        return getStringSetting(settingName).split(",");
    }
}
