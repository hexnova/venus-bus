package com.meidusa.venus.bus.xml;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.digester.Digester;
import org.apache.commons.digester.RuleSet;
import org.apache.commons.digester.xmlrules.FromXmlRuleSet;
import org.apache.commons.lang.StringUtils;

import com.meidusa.toolkit.common.bean.config.ConfigUtil;
import com.meidusa.toolkit.common.bean.config.ConfigurationException;
import com.meidusa.toolkit.common.poolable.MultipleLoadBalanceObjectPool;
import com.meidusa.toolkit.common.util.Tuple;
import com.meidusa.toolkit.net.BackendConnectionPool;
import com.meidusa.toolkit.net.MultipleLoadBalanceBackendConnectionPool;
import com.meidusa.toolkit.net.PollingBackendConnectionPool;
import com.meidusa.toolkit.util.StringUtil;
import com.meidusa.venus.bus.AbstractRemoteServiceManager;
import com.meidusa.venus.bus.config.bean.BusConfig;
import com.meidusa.venus.bus.config.bean.RemoteServiceConfig;
import com.meidusa.venus.bus.network.BusBackendConnectionFactory;
import com.meidusa.venus.bus.xml.bean.FactoryConfig;
import com.meidusa.venus.bus.xml.bean.Remote;
import com.meidusa.venus.digester.DigesterRuleParser;
import com.meidusa.venus.io.packet.PacketConstant;
import com.meidusa.venus.util.DefaultRange;
import com.meidusa.venus.util.Range;
import com.meidusa.venus.util.RangeUtil;

/**
 * 通过XML进行远程服务管理
 * 
 * @author structchen
 * 
 */
public class XmlFileRemoteServiceManager extends AbstractRemoteServiceManager {

    private String[] configFiles;

    public String[] getConfigFiles() {
        return configFiles;
    }

    public void setConfigFiles(String[] configFiles) {
        this.configFiles = configFiles;
    }

    private Map<String, BackendConnectionPool> initRemoteMap(Map<String, Remote> remots) throws Exception {
        Map<String, BackendConnectionPool> poolMap = new HashMap<String, BackendConnectionPool>();
        for (Map.Entry<String, Remote> entry : remots.entrySet()) {
            Remote remote = entry.getValue();
            FactoryConfig factoryConfig = remote.getFactory();
            if (factoryConfig == null || StringUtils.isEmpty(factoryConfig.getIpAddressList())) {
                throw new ConfigurationException("remote name=" + remote.getName() + " factory config is null or ipAddress is null");
            }
            String ipAddress = factoryConfig.getIpAddressList();
            String ipList[] = StringUtil.split(ipAddress, ", ");

            BackendConnectionPool nioPools[] = new PollingBackendConnectionPool[ipList.length];

            for (int i = 0; i < ipList.length; i++) {
                BusBackendConnectionFactory nioFactory = new BusBackendConnectionFactory();
                if (realPoolMap.get(ipList[i]) != null) {
                    nioPools[i] = realPoolMap.get(ipList[i]);
                    continue;
                }

                if (factoryConfig != null) {
                    BeanUtils.copyProperties(nioFactory, factoryConfig);
                }

                String temp[] = StringUtil.split(ipList[i], ":");
                if (temp.length > 1) {
                    nioFactory.setHost(temp[0]);
                    nioFactory.setPort(Integer.valueOf(temp[1]));
                } else {
                    nioFactory.setHost(temp[0]);
                    nioFactory.setPort(PacketConstant.VENUS_DEFAULT_PORT);
                }

                if (remote.getAuthenticator() != null) {
                    nioFactory.setAuthenticator(remote.getAuthenticator());
                }

                nioFactory.setConnector(this.getConnector());
                nioFactory.setMessageHandler(getMessageHandler());

                nioPools[i] = new PollingBackendConnectionPool(ipList[i], nioFactory, remote.getPoolSize());

                nioPools[i].init();
            }
            String poolName = remote.getName();

            MultipleLoadBalanceBackendConnectionPool nioPool = new MultipleLoadBalanceBackendConnectionPool(poolName,
                    MultipleLoadBalanceObjectPool.LOADBALANCING_ROUNDROBIN, nioPools);

            nioPool.init();
            poolMap.put(remote.getName(), nioPool);

        }

        return poolMap;
    }

    protected Map<String, List<Tuple<Range, BackendConnectionPool>>> load() throws Exception {
        BusConfig all = getBusConfig();

        Map<String, BackendConnectionPool> poolMap = initRemoteMap(all.getRemoteMap());

        Map<String, List<Tuple<Range, BackendConnectionPool>>> serviceMap = new HashMap<String, List<Tuple<Range, BackendConnectionPool>>>();

        // create objectPool
        for (RemoteServiceConfig config : all.getServices()) {
            BackendConnectionPool pool = null;
            if (!StringUtil.isEmpty(config.getRemote())) {
                pool = poolMap.get(config.getRemote());
                if (pool == null) {
                    throw new ConfigurationException("service=" + config.getServiceName() + ",remote not found:" + config.getRemote());
                }
            } else {
                String ipAddress = config.getIpAddressList();
                if (!StringUtil.isEmpty(ipAddress)) {
                    String ipList[] = StringUtil.split(config.getIpAddressList(), ", ");
                    pool = createVirtualPool(ipList, null);
                } else {
                    throw new ConfigurationException("Service or ipAddressList or remote can not be null:" + config.getServiceName());
                }
            }

            try {
                Tuple<Range, BackendConnectionPool> tuple = new Tuple<Range, BackendConnectionPool>();
                tuple.left = RangeUtil.getVersionRange(config.getVersion());
                if (tuple.left == null) {
                    tuple.left = new DefaultRange();
                }
                tuple.right = pool;

                List<Tuple<Range, BackendConnectionPool>> list = serviceMap.get(config.getServiceName());
                if (list == null) {
                    list = new ArrayList<Tuple<Range, BackendConnectionPool>>();
                    serviceMap.put(config.getServiceName(), list);
                }
                list.add(tuple);

            } catch (Exception e) {
                throw new ConfigurationException("init remote service config error:", e);
            }
        }
        return serviceMap;

    }

    /**
     * 
     * @return
     */
    protected BusConfig getBusConfig() {
        BusConfig all = new BusConfig();
        for (String configFile : configFiles) {
            configFile = (String) ConfigUtil.filter(configFile);
            RuleSet ruleSet = new FromXmlRuleSet(this.getClass().getResource("venusRemoteServiceRule.xml"), new DigesterRuleParser());
            Digester digester = new Digester();
            digester.setValidating(false);
            digester.addRuleSet(ruleSet);

            InputStream is = null;
            if (configFile.startsWith("classpath:")) {
                configFile = configFile.substring("classpath:".length());
                is = this.getClass().getClassLoader().getResourceAsStream(configFile);
                if (is == null) {
                    throw new ConfigurationException("configFile not found in classpath=" + configFile);
                }
            } else {
                if (configFile.startsWith("file:")) {
                    configFile = configFile.substring("file:".length());
                }
                try {
                    is = new FileInputStream(new File(configFile));
                } catch (FileNotFoundException e) {
                    throw new ConfigurationException("configFile not found with file=" + configFile, e);
                }
            }

            try {
                BusConfig venus = (BusConfig) digester.parse(is);
                for (RemoteServiceConfig config : venus.getServices()) {
                    if (config.getServiceName() == null) {
                        throw new ConfigurationException("Service name can not be null:" + configFile);
                    }
                }
                all.getRemoteMap().putAll(venus.getRemoteMap());
                all.getServices().addAll(venus.getServices());
            } catch (Exception e) {
                throw new ConfigurationException("can not parser xml:" + configFile, e);
            }
        }

        return all;
    }
}
