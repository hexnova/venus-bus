package com.meidusa.venus.bus.handler;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.meidusa.toolkit.common.util.Tuple;
import com.meidusa.toolkit.net.BackendConnectionPool;
import com.meidusa.venus.bus.ServiceRemoteManager;
import com.meidusa.venus.bus.network.BusBackendConnection;
import com.meidusa.venus.bus.network.BusFrontendConnection;
import com.meidusa.venus.exception.VenusExceptionCodeConstant;
import com.meidusa.venus.io.packet.AbstractServicePacket;
import com.meidusa.venus.io.packet.ErrorPacket;
import com.meidusa.venus.io.packet.ServiceAPIPacket;
import com.meidusa.venus.io.packet.ServicePacketBuffer;
import com.meidusa.venus.io.packet.VenusRouterPacket;
import com.meidusa.venus.io.packet.serialize.SerializeServiceRequestPacket;
import com.meidusa.venus.util.Range;

/**
 * 消息重试处理,诸如:后端服务不可用的 时候,将有默认3次尝试请求.每次间隔1秒的机制,重新对虚拟连接池发起请求,如果都失败将返回异常数据包给客户端.
 * 
 * @author structchen
 * 
 */
public class RetryMessageHandler {
    private static Logger logger = LoggerFactory.getLogger(BusFrontendMessageHandler.class);
    private static final int MAX_RETRY_TIMES = 3;
    private int maxRetryTimes = MAX_RETRY_TIMES;

    /**
     * 延迟对象接口
     * 
     * @author structchen
     * 
     */
    static class DelayedObject implements Delayed {
        private long time;

        private long nextFireTime = 0;

        public DelayedObject(long nsTime, TimeUnit timeUnit) {
            this.time = TimeUnit.NANOSECONDS.convert(nsTime, timeUnit);
            nextFireTime = time + System.nanoTime();
        }

        public void setDelayedTime(long time, TimeUnit timeUnit) {
            this.time = TimeUnit.NANOSECONDS.convert(time, timeUnit);
            nextFireTime = time + System.nanoTime();
        }

        public void reset() {
            nextFireTime = time + System.nanoTime();
        }

        public long getDelay(TimeUnit unit) {
            long d = unit.convert(nextFireTime - System.nanoTime(), TimeUnit.NANOSECONDS);
            return d;
        }

        public int compareTo(Delayed other) {
            if (other == this) {
                return 0;
            }
            DelayedObject x = (DelayedObject) other;
            long diff = nextFireTime - x.nextFireTime;
            if (diff < 0) {
                return -1;
            } else if (diff > 0) {
                return 1;
            } else {
                return 1;
            }
        }

    }

    /**
     * 路由消息的延迟队列对象实现
     * 
     * @author structchen
     * 
     */
    static class DelayedRouterMessage extends DelayedObject {
        private BusFrontendConnection conn;
        private VenusRouterPacket packet;
        private int times = 0;

        public DelayedRouterMessage(long nsTime, TimeUnit timeUnit, BusFrontendConnection conn, VenusRouterPacket packet) {
            super(nsTime, timeUnit);
            this.conn = conn;
            this.packet = packet;
        }

    }
    
    /**
     * 存放重试消息的队列
     */
    private BlockingQueue<DelayedRouterMessage> retryQueue = new DelayQueue<DelayedRouterMessage>();

    private ServiceRemoteManager remoteManager;

    public ServiceRemoteManager getRemoteManager() {
        return remoteManager;
    }

    public void setRemoteManager(ServiceRemoteManager remoteManager) {
        this.remoteManager = remoteManager;
    }

    public int getMaxRetryTimes() {
        return maxRetryTimes;
    }

    public void setMaxRetryTimes(int maxRetryTimes) {
        this.maxRetryTimes = maxRetryTimes;
    }

    public void handle(DelayedRouterMessage message) {
        BusFrontendConnection conn = message.conn;
        VenusRouterPacket routerPacket = message.packet;
        SerializeServiceRequestPacket request = null;
        try {

            ServiceAPIPacket apiPacket = new ServiceAPIPacket();
            ServicePacketBuffer packetBuffer = new ServicePacketBuffer(routerPacket.data);
            apiPacket.init(packetBuffer);
            final String apiName = apiPacket.apiName;
            int index = apiName.lastIndexOf(".");
            String serviceName = apiName.substring(0, index);
            // String methodName = apiName.substring(index + 1);
            List<Tuple<Range, BackendConnectionPool>> list = remoteManager.getRemoteList(serviceName);

            // service not found
            if (list == null || list.size() == 0) {
                ErrorPacket error = new ErrorPacket();
                AbstractServicePacket.copyHead(apiPacket, error);
                error.errorCode = VenusExceptionCodeConstant.SERVICE_NOT_FOUND;
                error.message = "service not found :" + serviceName;
                conn.write(error.toByteBuffer());
                return;
            }

            for (Tuple<Range, BackendConnectionPool> tuple : list) {

                /**
                 * 检测版本是否兼容
                 */
                if (tuple.left.contains(apiPacket.serviceVersion)) {
                    BusBackendConnection remoteConn = null;
                    try {
                        remoteConn = (BusBackendConnection) tuple.right.borrowObject();
                        long remoteRequestID = remoteConn.getNextRequestID();
                        routerPacket.backendRequestID = remoteRequestID;
                        remoteConn.addRequest(remoteRequestID, routerPacket.frontendConnectionID, routerPacket.frontendRequestID);

                        remoteConn.write(routerPacket.toByteBuffer());
                        return;
                    } catch (Exception e) {

                        if (message.times <= getMaxRetryTimes()) {
                            message.reset();
                            retryQueue.offer(message);
                            return;
                        } else {
                            logger.error("remote error api=" + apiName, e);
                            ErrorPacket error = new ErrorPacket();
                            AbstractServicePacket.copyHead(apiPacket, error);
                            error.errorCode = VenusExceptionCodeConstant.SERVICE_UNAVAILABLE_EXCEPTION;
                            error.message = "remote service exception:" + e.getMessage();
                            conn.write(error.toByteBuffer());
                        }
                        return;
                    } finally {
                        if (remoteConn != null) {
                            tuple.right.returnObject(remoteConn);
                        }
                    }
                }
            }

            // Service version not match
            ErrorPacket error = new ErrorPacket();
            AbstractServicePacket.copyHead(apiPacket, error);
            error.errorCode = VenusExceptionCodeConstant.SERVICE_VERSION_NOT_ALLOWD_EXCEPTION;
            error.message = "Service version not match";
            conn.write(error.toByteBuffer());

        } catch (Throwable e) {
            // 遇到问题则返回错误数据包
            ErrorPacket error = new ErrorPacket();
            AbstractServicePacket.copyHead(request, error);
            error.errorCode = VenusExceptionCodeConstant.SERVICE_UNAVAILABLE_EXCEPTION;
            error.message = e.getMessage();
            conn.write(error.toByteBuffer());
            logger.error("error when invoke", e);
            return;
        }
    }

    /**
     * 增加一个重试路由数据包
     * 
     * @param conn
     * @param data
     */
    public void addRetry(BusFrontendConnection conn, VenusRouterPacket data) {
        if (data != null) {
            retryQueue.offer(new DelayedRouterMessage(1, TimeUnit.SECONDS, conn, data));
        }
    }

    public void init() {
        /**
         * 采用一个线程来负责处理重新队列
         */
        new Thread() {
            {
                this.setDaemon(true);
            }

            public void run() {
                while (true) {
                    try {
                        DelayedRouterMessage data = retryQueue.take();

                        if (data.times > MAX_RETRY_TIMES) {
                            continue;
                        }

                        data.times++;

                        if (!data.conn.isClosed()) {
                            RetryMessageHandler.this.handle(data);
                        }

                    } catch (InterruptedException e) {
                        // ignore
                    }
                }
            }
        }.start();

    }

}
