package com.meidusa.venus.bus.registry;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.meidusa.toolkit.common.util.Tuple;
import com.meidusa.toolkit.net.BackendConnectionPool;
import com.meidusa.toolkit.util.StringUtil;
import com.meidusa.venus.bus.AbstractRemoteServiceManager;
import com.meidusa.venus.bus.VenusConnectionAcceptor;
import com.meidusa.venus.client.simple.SimpleServiceFactory;
import com.meidusa.venus.exception.VenusExceptionFactory;
import com.meidusa.venus.io.authenticate.Authenticator;
import com.meidusa.venus.service.registry.ServiceDefinition;
import com.meidusa.venus.service.registry.ServiceRegistry;
import com.meidusa.venus.util.DefaultRange;
import com.meidusa.venus.util.Range;
import com.meidusa.venus.util.RangeUtil;

/**
 * 通过注册中心进行登记的远程服务管理
 * 
 * @author Structchen
 * 
 */
@SuppressWarnings("rawtypes")
public class RegistryRemoteServiceManager extends AbstractRemoteServiceManager {
    private static Logger logger = LoggerFactory.getLogger(RegistryRemoteServiceManager.class);
    
    @Autowired
    private VenusConnectionAcceptor acceptor;
    /**
     * 注册中心主机IP
     */
    private String host;

    /**
     * 注册中心服务端口
     */
    private int port;

    /**
     * 与注册中心采用的认证方式
     */
    private Authenticator authenticator;

    private VenusExceptionFactory venusExceptionFactory;

    private List<ServiceDefinition> current;

    public VenusConnectionAcceptor getAcceptor() {
        return acceptor;
    }

    public void setAcceptor(VenusConnectionAcceptor acceptor) {
        this.acceptor = acceptor;
    }

    public String getHost() {
        return host;
    }
    
    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public Authenticator getAuthenticator() {
        return authenticator;
    }

    public void setAuthenticator(Authenticator authenticator) {
        this.authenticator = authenticator;
    }

    public VenusExceptionFactory getVenusExceptionFactory() {
        return venusExceptionFactory;
    }

    public void setVenusExceptionFactory(VenusExceptionFactory venusExceptionFactory) {
        this.venusExceptionFactory = venusExceptionFactory;
    }

    @Override
    protected Map<String, List<Tuple<Range, BackendConnectionPool>>> load() throws Exception {
        SimpleServiceFactory factory = new SimpleServiceFactory(host, port);
        if (authenticator != null) {
            factory.setAuthenticator(authenticator);
        }
        factory.setVenusExceptionFactory(venusExceptionFactory);
        final ServiceRegistry registry = factory.getService(ServiceRegistry.class);
        List<ServiceDefinition> list = registry.getServiceDefinitions();

        final Map<String, List<Tuple<Range, BackendConnectionPool>>> serviceMap = new HashMap<String, List<Tuple<Range, BackendConnectionPool>>>();

        for (ServiceDefinition definition : list) {
            List<Tuple<Range, BackendConnectionPool>> l = serviceMap.get(definition.getName());
            if (l == null) {
                l = new ArrayList<Tuple<Range, BackendConnectionPool>>();
                serviceMap.put(definition.getName(), l);
            }
            
            for(String ip:localAddress){
                definition.getIpAddress().remove(ip+":"+acceptor.getPort());
            }
            if(definition.getIpAddress().size()>0){
                String[] ips = definition.getIpAddress().toArray(new String[] {});
                BackendConnectionPool pool = this.createVirtualPool(ips, authenticator);
                Range range = RangeUtil.getVersionRange(definition.getVersionRange());
                Tuple<Range, BackendConnectionPool> tuple = new Tuple<Range, BackendConnectionPool>(range, pool);
                l.add(tuple);
            }
        }
        factory.destroy();
        this.current = list;
        new Thread() {
            {
                this.setDaemon(true);
            }

            public void run() {
                while (true) {
                    try {
                        Thread.sleep(60 * 1000);
                    } catch (InterruptedException e) {
                        // ignore
                    }
                    try {
                        SimpleServiceFactory factory = new SimpleServiceFactory(host, port);
                        if (authenticator != null) {
                            factory.setAuthenticator(authenticator);
                        }
                        factory.setVenusExceptionFactory(venusExceptionFactory);
                        final ServiceRegistry registry = factory.getService(ServiceRegistry.class);
                        List<ServiceDefinition> list = registry.getServiceDefinitions();
                        modifier(list, current);
                        current = list;
                        factory.destroy();
                        removeUnusedConnection(list);
                    } catch (Throwable e) {
                        logger.info("services  scheduled update error", e);
                    }
                }
            }

			private void removeUnusedConnection(List<ServiceDefinition> list) {
				if (list == null || list.size() == 0){
					return;
				}
				
				Map<String, List<Range>> serviceRangeMap = new HashMap<String, List<Range>>();
				
				for (ServiceDefinition sd : list){
					List<Range> ranges = serviceRangeMap.get(sd.getName());
					if (ranges == null){
						ranges = new ArrayList<Range>();
						serviceRangeMap.put(sd.getName(), ranges);
					}
					ranges.add(RangeUtil.getVersionRange(sd.getVersionRange()));
				}
				
				Iterator<String> iter = serviceMap.keySet().iterator();
				while(iter.hasNext()) {
					String serviceName = iter.next();
					List<Range> ranges = serviceRangeMap.get(serviceName);
					List<Tuple<Range, BackendConnectionPool>> tuples = serviceMap.get(serviceName);
					
					for(Tuple<Range, BackendConnectionPool> tuple: tuples) {
						if (tuple == null){
							continue;
						}
						if (ranges.contains(tuple.left)){
							continue;
						}
						
						try{
							logger.debug("close " + serviceName + (tuple.left instanceof DefaultRange ?  "无限制" : tuple.left));
							if (!tuple.right.isClosed()){
								tuple.right.close();
							}
						}catch(Exception e) {
							logger.error("close connection pool error:" , e);
						}
						
						iter.remove();
					}
					
				}
			}
        }.start();
        return serviceMap;
    }

    protected void update(ServiceDefinition newObj) {
        List<Tuple<Range, BackendConnectionPool>> list = serviceMap.get(newObj.getName());
        if (list == null) {
            list = new ArrayList<Tuple<Range, BackendConnectionPool>>();
            serviceMap.put(newObj.getName(), list);
        }

        for(String ip:localAddress){
            newObj.getIpAddress().remove(ip+":"+acceptor.getPort());
        }
        
        String[] ips = newObj.getIpAddress().toArray(new String[] {});
        Arrays.sort(ips);

        Range range = RangeUtil.getVersionRange(newObj.getVersionRange());

        boolean isNew = true;
        for (Tuple<Range, BackendConnectionPool> old : list) {
            if (old.left.equals(range)) {
                BackendConnectionPool pool = this.createVirtualPool(ips, authenticator);
                old.right = pool;
                isNew = false;
                logger.warn("update Service=" + newObj.getName() + ", new address=" + ArrayUtils.toString(ips));
                break;
            }
        }

        if (isNew) {
            BackendConnectionPool pool = this.createVirtualPool(ips, authenticator);
            Tuple<Range, BackendConnectionPool> tuple = new Tuple<Range, BackendConnectionPool>(range, pool);
            list.add(tuple);
            logger.warn("new Service=" + newObj.getName() + ", version=" + range + "address=" + ArrayUtils.toString(ips));
        }

    }

    /**
     * 
     * @param list
     * @param current
     */
    protected void modifier(List<ServiceDefinition> list, List<ServiceDefinition> current) {
        if (list == null) {
            return;
        }
        for (ServiceDefinition newObj : list) {
            boolean newService = true;
            boolean newVersion = true;
            boolean newHost = false;
            for (ServiceDefinition old : current) {

                Range newRange = RangeUtil.getVersionRange(newObj.getVersionRange());

                // 判断是否存在相同的服务
                if (StringUtil.equals(newObj.getName(), old.getName())) {

                    newService = false;
                    Range oldRange = RangeUtil.getVersionRange(old.getVersionRange());

                    // 是否存在相同的
                    if (newRange.equals(oldRange)) {
                        newVersion = false;
                        if (!newObj.getIpAddress().equals(old.getIpAddress())) {
                            newHost = true;
                        } else {
                            newHost = false;
                        }
                        break;
                    }
                }
            }

            // 如果存在新服务,新版本,新的ip地址,则需要更新
            if (newService || newVersion || newHost) {
                update(newObj);
            }
        }
        fixPools();
    }

}
