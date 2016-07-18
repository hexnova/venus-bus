package com.meidusa.venus.bus.network;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.meidusa.toolkit.util.TimeUtil;
import com.meidusa.venus.bus.handler.BusBackendMessageHandler;
import com.meidusa.venus.bus.handler.ClientConnectionObserver;
import com.meidusa.venus.bus.util.VenusTrafficCollector;
import com.meidusa.venus.io.network.VenusBackendConnection;
import com.meidusa.venus.io.packet.PingPacket;
import com.meidusa.venus.io.utils.Bits;

/**
 * 负责Bus后端连接
 * 
 * @author structchen
 * 
 */
public class BusBackendConnection extends VenusBackendConnection {
	private static Logger logger = LoggerFactory.getLogger(BusBackendConnection.class);
    private AtomicLong requestSeq = new AtomicLong();
    private final Map<Long, byte[]> unCompeleted = new ConcurrentHashMap<Long, byte[]>();
    private long lastPing;
    private long lastPong;
    private static long PING_INTERVAL = 15000;
    public long getLastPing() {
		return lastPing;
	}

	public void setLastPing(long lastPing) {
		this.lastPing = lastPing;
	}

	public long getLastPong() {
		return lastPong;
	}

	public void setLastPong(long lastPong) {
		this.lastPong = lastPong;
	}

	public BusBackendConnection(SocketChannel channel) {
        super(channel);
    }
	
	public void write(ByteBuffer buffer){
		VenusTrafficCollector.getInstance().addOutput(buffer.remaining());
		super.write(buffer);
	}
    
    /**
     * 
     * @return
     */
    public long getNextRequestID() {
        return requestSeq.getAndIncrement();
    }

    public boolean addRequest(long backendRequestId, long frontendConnID, long frontendRequestID) {
        byte[] tm = new byte[16];
        Bits.putLong(tm, 0, frontendConnID);
        Bits.putLong(tm, 8, frontendRequestID);
        unCompeleted.put(backendRequestId, tm);
        if (isClosed.get()) {
            unCompeleted.remove(backendRequestId);
            return false;
        } else {

            return true;
        }
    }
    
    protected void idleCheck() {
    	//避免在认证期间发送ping数据包
    	if(this.isAuthenticated()){
	        if (isIdleTimeout()) {
	        	logger.warn("conn="+this.host+":"+ this.port+ " ping/pong timeout="+(lastPing - lastPong)+"!");
	            close();
	        }else{
	        	PingPacket ping = new PingPacket();
	        	this.setLastPing(TimeUtil.currentTimeMillis());
	        	this.write(ping.toByteBuffer());
	        }
    	}
    }
    
    public boolean isIdleTimeout() {
        return lastPing - lastPong > PING_INTERVAL;
    }

    public boolean removeRequest(long requestID) {
        return unCompeleted.remove(requestID) != null;
    }

    public boolean close() {
        boolean closed = super.close();
        if (closed) {
            if (this.getHandler() instanceof BusBackendMessageHandler) {
                ClientConnectionObserver os = ((BusBackendMessageHandler) this.getHandler()).getClientConnectionObserver();

                Iterator<Entry<Long, byte[]>> it = unCompeleted.entrySet().iterator();
                while (it.hasNext()) {
                    Entry<Long, byte[]> item = it.next();
                    long frontendConnID = Bits.getLong(item.getValue(), 0);
                    long frontendRequestID = Bits.getLong(item.getValue(), 8);
                    BusFrontendConnection conn = (BusFrontendConnection) os.getConnection(frontendConnID);
                    if(conn != null && !conn.isClosed()){
                    	conn.retryRequestById(frontendRequestID);
                    }
                }

            }
        }
        return closed;
    }
}
