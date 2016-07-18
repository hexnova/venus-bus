package com.meidusa.venus.bus;

import com.meidusa.toolkit.common.runtime.Application;
import com.meidusa.toolkit.common.runtime.ApplicationConfig;
import com.meidusa.toolkit.common.runtime.DefaultApplication;

/**
 * 启动 BUS的 Application
 * 
 * @author structchen
 * 
 */
public class ServiceBusApplication extends DefaultApplication<ServiceBusApplicationConfig> {
    private ServiceBusApplicationConfig config = new ServiceBusApplicationConfig();

    @Override
    public void doRun() {

    }

    @Override
    public ServiceBusApplicationConfig getApplicationConfig() {
        return config;
    }

    public static void main(String[] args) {
        System.setProperty(ApplicationConfig.PROJECT_MAINCLASS, ServiceBusApplication.class.getName());
        Application.main(args);
    }
}
