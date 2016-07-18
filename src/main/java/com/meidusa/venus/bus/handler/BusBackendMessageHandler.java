package com.meidusa.venus.bus.handler;

import com.meidusa.toolkit.net.MessageHandler;
import com.meidusa.toolkit.util.TimeUtil;
import com.meidusa.venus.bus.network.BusBackendConnection;
import com.meidusa.venus.bus.network.BusFrontendConnection;
import com.meidusa.venus.bus.util.VenusTrafficCollector;
import com.meidusa.venus.io.packet.AbstractServicePacket;
import com.meidusa.venus.io.packet.AbstractVenusPacket;
import com.meidusa.venus.io.packet.VenusRouterPacket;

/**
 * 后端服务的 消息处理
 * 
 * @author structchen
 * 
 */
public class BusBackendMessageHandler implements MessageHandler<BusBackendConnection, byte[]> {
    private ClientConnectionObserver clientConnectionObserver;

    public ClientConnectionObserver getClientConnectionObserver() {
        return clientConnectionObserver;
    }

    public void setClientConnectionObserver(ClientConnectionObserver clientConnectionObserver) {
        this.clientConnectionObserver = clientConnectionObserver;
    }

    @Override
    public void handle(BusBackendConnection conn, byte[] message) {
    	VenusTrafficCollector.getInstance().addInput(message.length);
        int type = AbstractServicePacket.getType(message);
        if (type == AbstractVenusPacket.PACKET_TYPE_ROUTER) {

            BusFrontendConnection clientConn = (BusFrontendConnection) clientConnectionObserver.getConnection(VenusRouterPacket
                    .getConnectionSequenceID(message));
            conn.removeRequest(VenusRouterPacket.getRemoteRequestID(message));
            byte[] response = VenusRouterPacket.getData(message);
            if (clientConn != null && !clientConn.isClosed()) {
            	VenusRouterPacket router = clientConn.removeUnCompleted(VenusRouterPacket.getSourceRequestID(message));
            	if(router != null){
                    clientConn.write(response);
                }
            }
        }else if(type == AbstractVenusPacket.PACKET_TYPE_PONG){
        	conn.setLastPong(TimeUtil.currentTimeMillis());
        }

    }
}
