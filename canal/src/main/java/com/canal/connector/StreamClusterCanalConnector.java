package com.canal.connector;


import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalNodeAccessStrategy;
import com.alibaba.otter.canal.client.impl.ClusterNodeAccessStrategy;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.protocol.exception.CanalClientException;
import java.net.SocketAddress;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamClusterCanalConnector implements CanalConnector {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private String username;
    private String password;
    private int soTimeout = 10000;
    private int retryTimes = 3;
    private int retryInterval = 5000;
    private CanalNodeAccessStrategy accessStrategy;
    private StreamSimpleCanalConnector currentConnector;
    private String destination;
    private String filter;

    public StreamClusterCanalConnector(String username, String password, String destination, CanalNodeAccessStrategy accessStrategy) {
        this.username = username;
        this.password = password;
        this.destination = destination;
        this.accessStrategy = accessStrategy;
    }

    public synchronized void connect() throws CanalClientException {
        while(this.currentConnector == null) {
            int times = 0;

            while(true) {
                try {
                    this.currentConnector = new StreamSimpleCanalConnector((SocketAddress)null, this.username, this.password, this.destination) {
                        public SocketAddress getNextAddress() {
                            return StreamClusterCanalConnector.this.accessStrategy.nextNode();
                        }
                    };
                    this.currentConnector.setSoTimeout(this.soTimeout);
                    if (this.filter != null) {
                        this.currentConnector.setFilter(this.filter);
                    }

                    if (this.accessStrategy instanceof ClusterNodeAccessStrategy) {
                        this.currentConnector.setZkClientx(((ClusterNodeAccessStrategy)this.accessStrategy).getZkClient());
                    }

                    this.currentConnector.connect();
                    break;
                } catch (Exception var5) {
                    this.logger.warn("failed to connect to:{} after retry {} times", this.accessStrategy.currentNode(), times);
                    this.currentConnector.disconnect();
                    this.currentConnector = null;
                    ++times;
                    if (times >= this.retryTimes) {
                        throw new CanalClientException(var5);
                    }

                    try {
                        Thread.sleep((long)this.retryInterval);
                    } catch (InterruptedException var4) {
                        throw new CanalClientException(var4);
                    }
                }
            }
        }

    }

    public boolean checkValid() {
        return this.currentConnector != null && this.currentConnector.checkValid();
    }

    public synchronized void disconnect() throws CanalClientException {
        if (this.currentConnector != null) {
            this.currentConnector.disconnect();
            this.currentConnector = null;
        }

    }

    public void subscribe() throws CanalClientException {
        this.subscribe("");
    }

    public void subscribe(String filter) throws CanalClientException {
        int times = 0;

        while(times < this.retryTimes) {
            try {
                this.currentConnector.subscribe(filter);
                this.filter = filter;
                return;
            } catch (Throwable var4) {
                this.logger.warn("something goes wrong when subscribing from server:{}\n{}", this.currentConnector.getAddress(), ExceptionUtils.getFullStackTrace(var4));
                ++times;
                this.restart();
                this.logger.info("restart the connector for next round retry.");
            }
        }

        throw new CanalClientException("failed to subscribe after " + times + " times retry.");
    }

    public void unsubscribe() throws CanalClientException {
        int times = 0;

        while(times < this.retryTimes) {
            try {
                this.currentConnector.unsubscribe();
                return;
            } catch (Throwable var3) {
                this.logger.warn("something goes wrong when unsubscribing from server:{}\n{}", this.currentConnector.getAddress(), ExceptionUtils.getFullStackTrace(var3));
                ++times;
                this.restart();
                this.logger.info("restart the connector for next round retry.");
            }
        }

        throw new CanalClientException("failed to unsubscribe after " + times + " times retry.");
    }

    public Message get(int batchSize) throws CanalClientException {
        int times = 0;

        while(times < this.retryTimes) {
            try {
                Message msg = this.currentConnector.get(batchSize);
                return msg;
            } catch (Throwable var4) {
                this.logger.warn("something goes wrong when getting data from server:{}\n{}", this.currentConnector.getAddress(), ExceptionUtils.getFullStackTrace(var4));
                ++times;
                this.restart();
                this.logger.info("restart the connector for next round retry.");
            }
        }

        throw new CanalClientException("failed to fetch the data after " + times + " times retry");
    }

    public Message get(int batchSize, Long timeout, TimeUnit unit) throws CanalClientException {
        int times = 0;

        while(times < this.retryTimes) {
            try {
                Message msg = this.currentConnector.get(batchSize, timeout, unit);
                return msg;
            } catch (Throwable var6) {
                this.logger.warn("something goes wrong when getting data from server:{}\n{}", this.currentConnector.getAddress(), ExceptionUtils.getFullStackTrace(var6));
                ++times;
                this.restart();
                this.logger.info("restart the connector for next round retry.");
            }
        }

        throw new CanalClientException("failed to fetch the data after " + times + " times retry");
    }

    public Message getWithoutAck(int batchSize) throws CanalClientException {
        int times = 0;

        while(times < this.retryTimes) {
            try {
                Message msg = this.currentConnector.getWithoutAck(batchSize);
                return msg;
            } catch (Throwable var4) {
                this.logger.warn("something goes wrong when getWithoutAck data from server:{}\n{}", this.currentConnector.getAddress(), ExceptionUtils.getFullStackTrace(var4));
                ++times;
                this.restart();
                this.logger.info("restart the connector for next round retry.");
            }
        }

        throw new CanalClientException("failed to fetch the data after " + times + " times retry");
    }

    public Message getWithoutAck(int batchSize, Long timeout, TimeUnit unit) throws CanalClientException {
        int times = 0;

        while(times < this.retryTimes) {
            try {
                Message msg = this.currentConnector.getWithoutAck(batchSize, timeout, unit);
                return msg;
            } catch (Throwable var6) {
                this.logger.warn("something goes wrong when getWithoutAck data from server:{}\n{}", this.currentConnector.getAddress(), ExceptionUtils.getFullStackTrace(var6));
                ++times;
                this.restart();
                this.logger.info("restart the connector for next round retry.");
            }
        }

        throw new CanalClientException("failed to fetch the data after " + times + " times retry");
    }

    public void rollback(long batchId) throws CanalClientException {
        int times = 0;

        while(times < this.retryTimes) {
            try {
                this.currentConnector.rollback(batchId);
                return;
            } catch (Throwable var5) {
                this.logger.warn("something goes wrong when rollbacking data from server:{}\n{}", this.currentConnector.getAddress(), ExceptionUtils.getFullStackTrace(var5));
                ++times;
                this.restart();
                this.logger.info("restart the connector for next round retry.");
            }
        }

        throw new CanalClientException("failed to rollback after " + times + " times retry");
    }

    public void rollback() throws CanalClientException {
        int times = 0;

        while(times < this.retryTimes) {
            try {
                this.currentConnector.rollback();
                return;
            } catch (Throwable var3) {
                this.logger.warn("something goes wrong when rollbacking data from server:{}\n{}", this.currentConnector.getAddress(), ExceptionUtils.getFullStackTrace(var3));
                ++times;
                this.restart();
                this.logger.info("restart the connector for next round retry.");
            }
        }

        throw new CanalClientException("failed to rollback after " + times + " times retry");
    }

    public void ack(long batchId) throws CanalClientException {
        int times = 0;

        while(times < this.retryTimes) {
            try {
                this.currentConnector.ack(batchId);
                return;
            } catch (Throwable var5) {
                this.logger.warn("something goes wrong when acking data from server:{}\n{}", this.currentConnector.getAddress(), ExceptionUtils.getFullStackTrace(var5));
                ++times;
                this.restart();
                this.logger.info("restart the connector for next round retry.");
            }
        }

        throw new CanalClientException("failed to ack after " + times + " times retry");
    }

    private synchronized void restart() throws CanalClientException {
        this.disconnect();

        try {
            Thread.sleep((long)this.retryInterval);
        } catch (InterruptedException var2) {
            throw new CanalClientException(var2);
        }

        this.connect();
    }

    public String getUsername() {
        return this.username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return this.password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public int getSoTimeout() {
        return this.soTimeout;
    }

    public void setSoTimeout(int soTimeout) {
        this.soTimeout = soTimeout;
    }

    public int getRetryTimes() {
        return this.retryTimes;
    }

    public void setRetryTimes(int retryTimes) {
        this.retryTimes = retryTimes;
    }

    public int getRetryInterval() {
        return this.retryInterval;
    }

    public void setRetryInterval(int retryInterval) {
        this.retryInterval = retryInterval;
    }

    public CanalNodeAccessStrategy getAccessStrategy() {
        return this.accessStrategy;
    }

    public void setAccessStrategy(CanalNodeAccessStrategy accessStrategy) {
        this.accessStrategy = accessStrategy;
    }

    public StreamSimpleCanalConnector getCurrentConnector() {
        return this.currentConnector;
    }
}

