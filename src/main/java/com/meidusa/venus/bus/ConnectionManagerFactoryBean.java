package com.meidusa.venus.bus;

import java.util.concurrent.atomic.AtomicInteger;

import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;

import com.meidusa.toolkit.net.ConnectionManager;

/**
 * Spring factory bean,作为Connection Manager的工厂,每个 manager 默认的执行线程为 CPU core 数量
 * 
 * @author structchen
 * 
 */
public class ConnectionManagerFactoryBean implements FactoryBean<ConnectionManager[]>, InitializingBean {
    private static AtomicInteger index = new AtomicInteger();
    /**
     * 每个 manager创建以后,默认执行线程数量
     */
    private int executorSize = Runtime.getRuntime().availableProcessors();

    /**
     * Manager的名称前缀
     */
    private String prefix = "Manager";

    /**
     * 创建多少个Manager,默认为CPU Core数量
     */
    private int size = Runtime.getRuntime().availableProcessors();

    private ConnectionManager[] items;

    /**
     * 是否是单例
     */
    private boolean singleton = true;

    public void setSingleton(boolean singleton) {
        this.singleton = singleton;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public String getPrefix() {
        return prefix;
    }

    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }

    public int getExecutorSize() {
        return executorSize;
    }

    public void setExecutorSize(int executorSize) {
        this.executorSize = executorSize;
    }

    @Override
    public ConnectionManager[] getObject() throws Exception {
        int current = index.getAndIncrement();
        if (singleton) {
            return items;
        } else {
            ConnectionManager[] items = new ConnectionManager[size];
            for (int i = 0; i < size; i++) {
                items[i] = new ConnectionManager(this.getPrefix() + "-" + current + "-" + i, this.getExecutorSize());
                items[i].start();
            }
            return items;
        }
    }

    @Override
    public Class<?> getObjectType() {
        return ConnectionManager[].class;
    }

    @Override
    public boolean isSingleton() {
        return singleton;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        if (singleton) {
            items = new ConnectionManager[size];
            for (int i = 0; i < size; i++) {
                items[i] = new ConnectionManager(this.getPrefix() + "-" + i, this.getExecutorSize());
                items[i].start();
            }
        }
    }

}
