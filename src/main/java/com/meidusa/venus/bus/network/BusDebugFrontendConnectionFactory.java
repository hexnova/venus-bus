package com.meidusa.venus.bus.network;

import java.nio.channels.SocketChannel;

import com.meidusa.toolkit.net.AuthingableFrontendConnection;
import com.meidusa.toolkit.net.ConnectionConnector;
import com.meidusa.toolkit.net.FrontendConnection;
import com.meidusa.toolkit.net.authenticate.server.AuthenticateProvider;
import com.meidusa.toolkit.net.factory.FrontendConnectionFactory;

public class BusDebugFrontendConnectionFactory extends FrontendConnectionFactory {
    private String remoteHost;
    private int remotePort;
    private ConnectionConnector connector;
    private AuthenticateProvider<AuthingableFrontendConnection, byte[]> authenticateProvider;

    @Override
    protected FrontendConnection getConnection(SocketChannel channel) {
        BusDebugFrontendConnection conn = new BusDebugFrontendConnection(channel, connector, remoteHost, remotePort);
        conn.setHandler(conn);
        conn.setRequestHandler(conn);
        return conn;
    }

    /**
     * 
     * @return
     */
    public AuthenticateProvider<AuthingableFrontendConnection, byte[]> getAuthenticateProvider() {
        return authenticateProvider;
    }

    public void setAuthenticateProvider(AuthenticateProvider<AuthingableFrontendConnection, byte[]> authenticateProvider) {
        this.authenticateProvider = authenticateProvider;
    }

    public String getRemoteHost() {
        return remoteHost;
    }

    public void setRemoteHost(String remoteHost) {
        this.remoteHost = remoteHost;
    }

    public int getRemotePort() {
        return remotePort;
    }

    public void setRemotePort(int remotePort) {
        this.remotePort = remotePort;
    }

    public ConnectionConnector getConnector() {
        return connector;
    }

    public void setConnector(ConnectionConnector connector) {
        this.connector = connector;
    }

}
