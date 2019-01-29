package com.canal.connector;


import com.alibaba.otter.canal.client.impl.running.ClientRunningData;
import com.alibaba.otter.canal.client.impl.running.ClientRunningListener;
import com.alibaba.otter.canal.common.AbstractCanalLifeCycle;
import com.alibaba.otter.canal.common.utils.BooleanMutex;
import com.alibaba.otter.canal.common.utils.JsonUtils;
import com.alibaba.otter.canal.common.zookeeper.ZkClientx;
import com.alibaba.otter.canal.common.zookeeper.ZookeeperPathUtils;
import com.alibaba.otter.canal.protocol.exception.CanalClientException;
import java.net.InetSocketAddress;
import java.text.MessageFormat;
import java.util.concurrent.TimeUnit;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.exception.ZkException;
import org.I0Itec.zkclient.exception.ZkInterruptedException;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

public class StreamClientRunningMonitor extends AbstractCanalLifeCycle {
    private static final Logger logger = LoggerFactory.getLogger(StreamClientRunningMonitor.class);
    private ZkClientx zkClient;
    private String destination;
    private ClientRunningData clientData;
    private IZkDataListener dataListener = new IZkDataListener() {
        @Override
        public void handleDataChange(String dataPath, Object data) throws Exception {
            MDC.put("destination", StreamClientRunningMonitor.this.destination);
            ClientRunningData runningData = (ClientRunningData)JsonUtils.unmarshalFromByte((byte[])((byte[])data), ClientRunningData.class);
            if (!StreamClientRunningMonitor.this.isMine(runningData.getAddress())) {
                StreamClientRunningMonitor.this.mutex.set(false);
            }

            if (!runningData.isActive() && StreamClientRunningMonitor.this.isMine(runningData.getAddress())) {
                StreamClientRunningMonitor.this.release = true;
                StreamClientRunningMonitor.this.releaseRunning();
            }

            StreamClientRunningMonitor.this.activeData = runningData;
        }

        public void handleDataDeleted(String dataPath) throws Exception {
            MDC.put("destination", StreamClientRunningMonitor.this.destination);
            StreamClientRunningMonitor.this.mutex.set(false);
            StreamClientRunningMonitor.this.processActiveExit();
            if (!StreamClientRunningMonitor.this.release && StreamClientRunningMonitor.this.activeData != null && StreamClientRunningMonitor.this.isMine(StreamClientRunningMonitor.this.activeData.getAddress())) {
                StreamClientRunningMonitor.this.initRunning();
            } else {
                TimeUnit.SECONDS.sleep((long)StreamClientRunningMonitor.this.delayTime);
                StreamClientRunningMonitor.this.initRunning();
            }

        }
    };
    private BooleanMutex mutex = new BooleanMutex(false);
    private volatile boolean release = false;
    private volatile ClientRunningData activeData;
    private ClientRunningListener listener;
    private int delayTime = 5;

    public StreamClientRunningMonitor() {
    }

    public void start() {
        super.start();
        String path = ZookeeperPathUtils.getDestinationClientRunning(this.destination, this.clientData.getClientId());
        this.zkClient.subscribeDataChanges(path, this.dataListener);
        this.initRunning();
    }

    public void stop() {
        super.stop();
        String path = ZookeeperPathUtils.getDestinationClientRunning(this.destination, this.clientData.getClientId());
        this.zkClient.unsubscribeDataChanges(path, this.dataListener);
        this.releaseRunning();
    }

    public synchronized void initRunning() {
        if (this.isStart()) {
            String path = ZookeeperPathUtils.getDestinationClientRunning(this.destination, this.clientData.getClientId());
            byte[] bytes = JsonUtils.marshalToByte(this.clientData);

            try {
                this.mutex.set(false);
                this.zkClient.create(path, bytes, CreateMode.EPHEMERAL);
                this.processActiveEnter();
                this.activeData = this.clientData;
                this.mutex.set(true);
            } catch (ZkNodeExistsException var4) {
                bytes = (byte[])this.zkClient.readData(path, true);
                if (bytes == null) {
                    this.initRunning();
                } else {
                    this.activeData = (ClientRunningData)JsonUtils.unmarshalFromByte(bytes, ClientRunningData.class);
                    if (this.activeData.getAddress().contains(":") && this.isMine(this.activeData.getAddress())) {
                        this.mutex.set(true);
                    }
                }
            } catch (ZkNoNodeException var5) {
                this.zkClient.createPersistent(ZookeeperPathUtils.getClientIdNodePath(this.destination, this.clientData.getClientId()), true);
                this.initRunning();
            } catch (Throwable var6) {
                logger.error(MessageFormat.format("There is an error when execute initRunning method, with destination [{0}].", this.destination), var6);
                this.releaseRunning();
                throw new CanalClientException("something goes wrong in initRunning method. ", var6);
            }

        }
    }

    public void waitForActive() throws InterruptedException {
        this.initRunning();
        this.mutex.get();
    }

    public boolean check() {
        String path = ZookeeperPathUtils.getDestinationClientRunning(this.destination, this.clientData.getClientId());

        try {
            byte[] bytes = (byte[])this.zkClient.readData(path);
            ClientRunningData eventData = (ClientRunningData)JsonUtils.unmarshalFromByte(bytes, ClientRunningData.class);
            this.activeData = eventData;
            boolean result = this.isMine(this.activeData.getAddress());
            if (!result) {
                logger.warn("canal is running in [{}] , but not in [{}]", this.activeData.getAddress(), this.clientData.getAddress());
            }

            return result;
        } catch (ZkNoNodeException var5) {
            logger.warn("canal is not run any in node");
            return false;
        } catch (ZkInterruptedException var6) {
            logger.warn("canal check is interrupt");
            Thread.interrupted();
            return this.check();
        } catch (ZkException var7) {
            logger.warn("canal check is failed");
            return false;
        }
    }

    public boolean releaseRunning() {
        if (this.check()) {
            String path = ZookeeperPathUtils.getDestinationClientRunning(this.destination, this.clientData.getClientId());
            this.zkClient.delete(path);
            this.mutex.set(false);
            this.processActiveExit();
            return true;
        } else {
            return false;
        }
    }

    private boolean isMine(String address) {
        return address.equals(this.clientData.getAddress());
    }

    private void processActiveEnter() {
        if (this.listener != null) {
            InetSocketAddress connectAddress = this.listener.processActiveEnter();
            String address = connectAddress.getAddress().getHostAddress() + ":" + connectAddress.getPort();
            this.clientData.setAddress(address);
            String path = ZookeeperPathUtils.getDestinationClientRunning(this.destination, this.clientData.getClientId());
            byte[] bytes = JsonUtils.marshalToByte(this.clientData);
            this.zkClient.writeData(path, bytes);
        }

    }

    private void processActiveExit() {
        if (this.listener != null) {
            this.listener.processActiveExit();
        }

    }

    public void setListener(ClientRunningListener listener) {
        this.listener = listener;
    }

    public void setDestination(String destination) {
        this.destination = destination;
    }

    public void setClientData(ClientRunningData clientData) {
        this.clientData = clientData;
    }

    public void setDelayTime(int delayTime) {
        this.delayTime = delayTime;
    }

    public void setZkClient(ZkClientx zkClient) {
        this.zkClient = zkClient;
    }
}

