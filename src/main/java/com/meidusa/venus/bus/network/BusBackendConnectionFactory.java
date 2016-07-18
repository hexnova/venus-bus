package com.meidusa.venus.bus.network;

import java.nio.channels.SocketChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.meidusa.toolkit.common.bean.util.Initialisable;
import com.meidusa.toolkit.common.bean.util.InitialisationException;
import com.meidusa.toolkit.common.heartbeat.Status;
import com.meidusa.toolkit.net.BackendConnection;
import com.meidusa.toolkit.net.ValidatorMessageHandler;
import com.meidusa.venus.io.network.VenusBackendConnectionFactory;
import com.meidusa.venus.io.packet.AbstractServicePacket;
import com.meidusa.venus.io.packet.AbstractVenusPacket;
import com.meidusa.venus.io.packet.PacketConstant;
import com.meidusa.venus.io.packet.VenusStatusRequestPacket;
import com.meidusa.venus.io.packet.VenusStatusResponsePacket;

/**
 * 后端连接工厂
 * 
 * @author structchen
 * 
 */
public class BusBackendConnectionFactory extends VenusBackendConnectionFactory implements Initialisable {
    private static Logger logger = LoggerFactory.getLogger(BusBackendConnectionFactory.class);

    @Override
    protected BackendConnection create(SocketChannel channel) {
        BusBackendConnection c = new BusBackendConnection(channel);
        c.setResponseMessageHandler(getMessageHandler());
        /*
         * if(Thread.currentThread() instanceof ConnectionManager){ ConnectionManager manager =
         * (ConnectionManager)Thread.currentThread(); c.setProcessor(manager); }
         */
        return c;
    }
    
    @Override
    public void init() throws InitialisationException {
        logger.info("backend socket receiveBuffer=" + this.getReceiveBufferSize() + "K, sentBuffer=" + this.getSendBufferSize() + "K");
    }
    
	public ValidatorMessageHandler<byte[]> createValidatorMessageHandler() {
		return new ValidatorMessageHandler<byte[]>(){
			
			@Override
			public void handle(BackendConnection conn, byte[] message) {
				int type = AbstractServicePacket.getType(message);
		        if(type == AbstractVenusPacket.PACKET_TYPE_PONG){
		        	this.setStatus(Status.VALID);;
		        }else if(type == AbstractVenusPacket.PACKET_TYPE_VENUS_STATUS_RESPONSE){
		        	VenusStatusResponsePacket packet = new VenusStatusResponsePacket();
		        	packet.init(message);
		        	if((packet.status & PacketConstant.VENUS_STATUS_OUT_OF_MEMORY) >0){
		        		this.setStatus(Status.OUT_OF_MEMORY);
		        	}else if((packet.status & PacketConstant.VENUS_STATUS_SHUTDOWN) > 0){
		        		this.setStatus(Status.INVALID);
		        	}else{
		        		this.setStatus(Status.VALID);
		        	}
		        }
			}

			@Override
			protected void doCheck(BackendConnection conn) {
				VenusStatusRequestPacket packet = new VenusStatusRequestPacket();
				conn.write(packet.toByteBuffer());
			}

			
		};
	}
}
