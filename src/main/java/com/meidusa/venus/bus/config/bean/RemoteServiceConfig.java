package com.meidusa.venus.bus.config.bean;

/**
 * 
 * @author structchen
 * 
 */
public class RemoteServiceConfig {
    private String remote;
    private String serviceName;
    private String version;
    private String ipAddressList;
    private int soTimeout;

    public String getRemote() {
        return remote;
    }

    public void setRemote(String remote) {
        this.remote = remote;
    }

    public String getServiceName() {
        return serviceName;
    }

    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String versionRange) {
        this.version = versionRange;
    }

    public String getIpAddressList() {
        return ipAddressList;
    }

    public void setIpAddressList(String ipAddressList) {
        this.ipAddressList = ipAddressList;
    }

    public int getSoTimeout() {
        return soTimeout;
    }

    public void setSoTimeout(int soTimeout) {
        this.soTimeout = soTimeout;
    }

}
