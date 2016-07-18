package com.meidusa.venus.bus.network;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Map;

import com.meidusa.venus.bus.handler.RetryMessageHandler;
import com.meidusa.venus.bus.util.VenusTrafficCollector;
import com.meidusa.venus.io.network.VenusFrontendConnection;
import com.meidusa.venus.io.packet.VenusRouterPacket;

/**
 * 负责Bus前端连接
 * 
 * @author structchen
 * 
 */
public class BusFrontendConnection extends VenusFrontendConnection {
    private long requestSeq = 0L;
    private RetryMessageHandler retryHandler;
    private Map<Long, VenusRouterPacket> unCompleted = new HashMap<Long, VenusRouterPacket>();

    public BusFrontendConnection(SocketChannel channel) {
        super(channel);
    }
    
    public void addUnCompleted(long requestId, VenusRouterPacket data) {
        unCompleted.put(requestId, data);
    }

    public VenusRouterPacket removeUnCompleted(long requestId) {
        return unCompleted.remove(requestId);
    }

    public long getNextRequestID() {
        return requestSeq++;
    }

    public RetryMessageHandler getRetryHandler() {
        return retryHandler;
    }

    public void setRetryHandler(RetryMessageHandler retryHandler) {
        this.retryHandler = retryHandler;
    }

    public void retryRequestById(long requestID) {
        if (!this.isClosed()) {
            retryHandler.addRetry(this, unCompleted.get(requestID));
        }
    }
    
    public void write(ByteBuffer buffer){
    	VenusTrafficCollector.getInstance().addOutput(buffer.remaining());
		super.write(buffer);
	}

}
