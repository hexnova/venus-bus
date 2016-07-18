package com.meidusa.venus.bus.authenticate;

import com.meidusa.venus.backend.authenticate.AbstractAuthenticateProvider;
import com.meidusa.venus.io.network.VenusFrontendConnection;

/**
 * 
 * @author structchen
 * @param <T>
 */
public class SimpleAuthenticateProvider<T extends VenusFrontendConnection> extends AbstractAuthenticateProvider<T,byte[]> {

    @Override
    protected byte[] transferData(byte[] data) {
        return data;
    }
}
