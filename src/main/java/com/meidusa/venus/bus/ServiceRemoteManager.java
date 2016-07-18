package com.meidusa.venus.bus;

import java.util.List;

import com.meidusa.toolkit.common.util.Tuple;
import com.meidusa.toolkit.net.BackendConnectionPool;
import com.meidusa.venus.util.Range;

/**
 * Service Manager Interface
 * 
 * @author structchen
 * 
 */
public interface ServiceRemoteManager {
    List<Tuple<Range, BackendConnectionPool>> getRemoteList(String serviceName);
}
