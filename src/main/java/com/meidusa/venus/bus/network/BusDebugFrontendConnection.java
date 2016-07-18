package com.meidusa.venus.bus.network;

import java.io.IOException;
import java.nio.channels.SocketChannel;

import com.meidusa.toolkit.net.Connection;
import com.meidusa.toolkit.net.ConnectionConnector;
import com.meidusa.toolkit.net.ConnectionManager;
import com.meidusa.toolkit.net.MessageHandler;
import com.meidusa.toolkit.net.buffer.BufferQueue;
import com.meidusa.venus.io.network.VenusBackendConnection;
import com.meidusa.venus.io.network.VenusFrontendConnection;

/**
 * 
 * @author structchen
 * 
 */
public class BusDebugFrontendConnection extends VenusFrontendConnection implements MessageHandler<Connection, byte[]> {
    private static final int QUEUE_CAPCITY = 10;
    private VenusBackendConnection backendConn;
    private String remoteHost;
    private int remotePort;
    private ConnectionConnector connector;

    public BusDebugFrontendConnection(SocketChannel channel, ConnectionConnector connector, String remoteHost, int remotePort) {
        super(channel);
        this.remoteHost = remoteHost;
        this.remotePort = remotePort;
        this.setRequestHandler(this);
        this.setHandler(this);
        this.setAuthenticated(true);
        this.connector = connector;
    }

    @Override
    public void handle(Connection conn, byte[] data) {
        if (conn == this) {
            backendConn.write(data);
        } else {
            this.write(data);
        }
    }

    public void setProcessor(ConnectionManager processor) {
        super.setProcessor(processor);

        SocketChannel serChannel = null;
        try {
            serChannel = SocketChannel.open();
            serChannel.configureBlocking(false);
            serChannel.socket().setTcpNoDelay(true);
            serChannel.socket().setKeepAlive(true);
        } catch (IOException e) {
            e.printStackTrace();
        }

        backendConn = new VenusBackendConnection(serChannel);
        backendConn.setHandler(this);
        backendConn.setWriteQueue(new BufferQueue(QUEUE_CAPCITY));
        backendConn.setHost(remoteHost);
        backendConn.setPort(remotePort);
        backendConn.setResponseMessageHandler(this);
        this.connector.postConnect(backendConn);
    }

    @Override
    public boolean close() {
        if (super.close()) {
            backendConn.close();
            return true;
        }
        return false;
    }
}
