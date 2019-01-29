package com.canal.client;


import com.alibaba.otter.canal.client.CanalConnectors;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;

public class ClusterCanalClient extends AbstractCanalClient implements InitializingBean, DisposableBean {
    protected String zkAddress;
    protected String username;
    protected String password;

    public ClusterCanalClient() {
    }
    @Override
    public void destroy() throws Exception {
        this.stop();
    }
    @Override
    protected void process() {
        super.process();
    }
    @Override
    public void afterPropertiesSet() throws Exception {
        this.setConnector(CanalConnectors.newClusterConnector(this.zkAddress, this.destination, this.username, this.password));
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

