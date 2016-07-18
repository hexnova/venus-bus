package com.meidusa.venus.bus.packet;

import java.io.UnsupportedEncodingException;

import com.meidusa.venus.io.packet.AbstractServiceRequestPacket;
import com.meidusa.venus.io.packet.ServicePacketBuffer;

/**
 * 
 * @author structchen
 * 
 */
public class BusServiceRequestPacket extends AbstractServiceRequestPacket {

    private static final long serialVersionUID = 1L;
    private byte[] parameterBytes;

    protected void readBody(ServicePacketBuffer buffer) {
        super.readBody(buffer);
        if (buffer.hasRemaining()) {
            parameterBytes = buffer.getBytes(buffer.remaining());
        }
    }

    protected void writeBody(ServicePacketBuffer buffer) throws UnsupportedEncodingException {
        super.writeBody(buffer);
        if (parameterBytes != null) {
            buffer.writeBytes(parameterBytes);
        }
    }
}
