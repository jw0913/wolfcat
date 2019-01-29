package com.canal.client;


import com.alibaba.otter.canal.client.impl.ClusterNodeAccessStrategy;
import com.alibaba.otter.canal.common.zookeeper.ZkClientx;
import com.canal.connector.StreamClusterCanalConnector;
import org.springframework.beans.factory.DisposableBean;

public class StreamClusterCanalClient extends StreamCanalClient implements DisposableBean {
    protected String zkAddress;
    protected String username;
    protected String password;

    public StreamClusterCanalClient() {
    }

    public void destroy() throws Exception {
        this.stop();
    }

    protected void process() {
        super.process();
    }

    public void afterPropertiesSet() throws Exception {
        super.afterPropertiesSet();
        StreamClusterCanalConnector canalConnector = new StreamClusterCanalConnector(this.username, this.password, this.destination, new ClusterNodeAccessStrategy(this.destination, ZkClientx.getZkClient(this.zkAddress)));
        canalConnector.setSoTimeout(30000);
        this.setConnector(canalConnector);
        this.start();
    }

    public void setZkAddress(String zkAddress) {
        this.zkAddress = zkAddress;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public void setPassword(String password) {
        this.password = password;
    }
}

