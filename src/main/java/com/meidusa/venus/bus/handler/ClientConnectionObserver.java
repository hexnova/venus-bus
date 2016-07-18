package com.meidusa.venus.bus.handler;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.meidusa.toolkit.net.Connection;
import com.meidusa.toolkit.net.ConnectionObserver;
import com.meidusa.venus.io.network.VenusFrontendConnection;

/**
 * Client Connection Ovserver 观察连接创建,关闭以及异常
 * 
 * @author structchen
 * 
 */
public class ClientConnectionObserver implements ConnectionObserver {
    private Map<Long, Connection> clientMap = new ConcurrentHashMap<Long, Connection>();

    @Override
    public void connectionClosed(Connection conn) {
        if (conn instanceof VenusFrontendConnection) {
            clientMap.remove(((VenusFrontendConnection) conn).getSequenceID());
        }
    }

    @Override
    public void connectionEstablished(Connection conn) {
        if (conn instanceof VenusFrontendConnection) {
            clientMap.put(((VenusFrontendConnection) conn).getSequenceID(), conn);
        }
    }

    @Override
    public void connectionFailed(Connection conn, Exception fault) {
        if (conn instanceof VenusFrontendConnection) {
            clientMap.remove(((VenusFrontendConnection) conn).getSequenceID());
        }
    }

    /**
     * 根据 SequenceID 获得一个 连接
     * 
     * @param id
     * @return
     */
    public Connection getConnection(long id) {
        return clientMap.get(id);
    }
}
