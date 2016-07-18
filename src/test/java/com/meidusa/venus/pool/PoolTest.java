package com.meidusa.venus.pool;

import java.io.IOException;
import java.util.Arrays;

import com.meidusa.toolkit.common.poolable.MultipleLoadBalanceObjectPool;
import com.meidusa.toolkit.net.BackendConnection;
import com.meidusa.toolkit.net.BackendConnectionPool;
import com.meidusa.toolkit.net.ConnectionConnector;
import com.meidusa.toolkit.net.MultipleLoadBalanceBackendConnectionPool;
import com.meidusa.toolkit.net.PollingBackendConnectionPool;
import com.meidusa.toolkit.util.StringUtil;
import com.meidusa.venus.bus.network.BusBackendConnectionFactory;
import com.meidusa.venus.io.authenticate.Authenticator;
import com.meidusa.venus.io.packet.PacketConstant;
import com.meidusa.venus.io.packet.PingPacket;

public class PoolTest {

	
    /**
     * 创建多个真实地址连接的虚拟连接池
     * 
     * @param ipList
     * @return
     * @throws IOException 
     */
    protected static synchronized BackendConnectionPool createVirtualPool(String ipList[], Authenticator authenticator) throws IOException {
        Arrays.sort(ipList);
        String poolName = "Virtual-" + Arrays.toString(ipList);
        BackendConnectionPool[] nioPools = new PollingBackendConnectionPool[ipList.length];

        for (int i = 0; i < ipList.length; i++) {
        	nioPools[i] = createRealPool(ipList[i], authenticator);
        }

        MultipleLoadBalanceBackendConnectionPool multiPool = new MultipleLoadBalanceBackendConnectionPool(poolName, MultipleLoadBalanceObjectPool.LOADBALANCING_ROUNDROBIN, nioPools);
        multiPool.init();
        return multiPool;
    }
    
    /**
     * 创建与真实地址连接的连接池,该连接池创建完成以后,需要将它放入 realPoolMap
     * 
     * @param address 格式 host:port
     * @return BackendConnectionPool
     * @throws IOException 
     */
    protected static BackendConnectionPool createRealPool(String address, @SuppressWarnings("rawtypes") Authenticator authenticator) throws IOException {
        BusBackendConnectionFactory nioFactory = new BusBackendConnectionFactory();
        if (authenticator != null) {
            nioFactory.setAuthenticator(authenticator);
        }
        BackendConnectionPool pool = null;
        String[] temp = StringUtil.split(address, ":");
        if (temp.length > 1) {
            nioFactory.setHost(temp[0]);
            nioFactory.setPort(Integer.valueOf(temp[1]));
        } else {
            nioFactory.setHost(temp[0]);
            nioFactory.setPort(PacketConstant.VENUS_DEFAULT_PORT);
        }

        ConnectionConnector connector = new ConnectionConnector("Manager");
        connector.start();
        nioFactory.setConnector(connector);
        NioMessageHandler handler = new NioMessageHandler();
        
        nioFactory.setMessageHandler(handler);

        pool = new PollingBackendConnectionPool(address, nioFactory, 8);
        pool.init();
        return pool;
    }
    
	public static void main(String[] args) throws Exception {
		
		BackendConnectionPool pool = createVirtualPool(new String[]{"127.0.0.1:16800"},null);
		while(true){
			BackendConnection object = null;
			try{
				object = pool.borrowObject();
				PingPacket pong = new PingPacket();
                object.write(pong.toByteBuffer());
			}catch(Exception e){
				e.printStackTrace();
			}finally{
				if(object != null){
					pool.returnObject(object);
				}
			}
			Thread.sleep(1000);
		}
	}

}
