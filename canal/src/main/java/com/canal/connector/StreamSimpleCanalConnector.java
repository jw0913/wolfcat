package com.canal.connector;


import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.impl.running.ClientRunningData;
import com.alibaba.otter.canal.client.impl.running.ClientRunningListener;
import com.alibaba.otter.canal.common.utils.AddressUtils;
import com.alibaba.otter.canal.common.utils.BooleanMutex;
import com.alibaba.otter.canal.common.zookeeper.ZkClientx;
import com.alibaba.otter.canal.protocol.ClientIdentity;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.protocol.CanalEntry.Entry;
import com.alibaba.otter.canal.protocol.CanalPacket.Ack;
import com.alibaba.otter.canal.protocol.CanalPacket.ClientAck;
import com.alibaba.otter.canal.protocol.CanalPacket.ClientAuth;
import com.alibaba.otter.canal.protocol.CanalPacket.ClientRollback;
import com.alibaba.otter.canal.protocol.CanalPacket.Compression;
import com.alibaba.otter.canal.protocol.CanalPacket.Get;
import com.alibaba.otter.canal.protocol.CanalPacket.Handshake;
import com.alibaba.otter.canal.protocol.CanalPacket.Messages;
import com.alibaba.otter.canal.protocol.CanalPacket.Packet;
import com.alibaba.otter.canal.protocol.CanalPacket.PacketType;
import com.alibaba.otter.canal.protocol.CanalPacket.Sub;
import com.alibaba.otter.canal.protocol.CanalPacket.Unsub;
import com.alibaba.otter.canal.protocol.exception.CanalClientException;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamSimpleCanalConnector implements CanalConnector {
    private static final Logger logger = LoggerFactory.getLogger(StreamSimpleCanalConnector.class);
    private SocketAddress address;
    private String username;
    private String password;
    private int soTimeout;
    private String filter;
    private final ByteBuffer readHeader;
    private final ByteBuffer writeHeader;
    private volatile SocketChannel channel;
    private List<Compression> supportedCompressions;
    private ClientIdentity clientIdentity;
    private volatile StreamClientRunningMonitor runningMonitor;
    private ZkClientx zkClientx;
    private volatile BooleanMutex mutex;
    private volatile boolean connected;
    private boolean rollbackOnConnect;
    private boolean rollbackOnDisConnect;
    private Object readDataLock;
    private Object writeDataLock;

    public StreamSimpleCanalConnector(SocketAddress address, String username, String password, String destination) {
        this(address, username, password, destination, 60000);
    }

    public StreamSimpleCanalConnector(SocketAddress address, String username, String password, String destination, int soTimeout) {
        this.soTimeout = 60000;
        this.readHeader = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN);
        this.writeHeader = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN);
        this.supportedCompressions = new ArrayList();
        this.mutex = new BooleanMutex(false);
        this.connected = false;
        this.rollbackOnConnect = true;
        this.rollbackOnDisConnect = false;
        this.readDataLock = new Object();
        this.writeDataLock = new Object();
        this.address = address;
        this.username = username;
        this.password = password;
        this.soTimeout = soTimeout;
        this.clientIdentity = new ClientIdentity(destination, (short)1001);
    }

    public synchronized void connect() throws CanalClientException {
        if (!this.connected) {
            if (this.runningMonitor != null) {
                if (!this.runningMonitor.isStart()) {
                    this.runningMonitor.start();
                }
            } else {
                this.waitClientRunning();
                this.doConnect();
                if (this.filter != null) {
                    this.subscribe(this.filter);
                }

                if (this.rollbackOnConnect) {
                    this.rollback();
                }
            }

            this.connected = true;
        }
    }

    public synchronized void disconnect() throws CanalClientException {
        if (this.rollbackOnDisConnect && this.channel.isConnected()) {
            this.rollback();
        }

        this.connected = false;
        if (this.runningMonitor != null) {
            if (this.runningMonitor.isStart()) {
                this.runningMonitor.stop();
            }
        } else {
            this.doDisconnnect();
        }

    }

    private synchronized InetSocketAddress doConnect() throws CanalClientException {
        try {
            this.channel = SocketChannel.open();
            this.channel.socket().setSoTimeout(this.soTimeout);
            SocketAddress address = this.getAddress();
            if (address == null) {
                address = this.getNextAddress();
            }

            this.channel.connect(address);
            Packet p = Packet.parseFrom(this.readNextPacket(this.channel));
            if (p.getVersion() != 1) {
                throw new CanalClientException("unsupported version at this client.");
            } else if (p.getType() != PacketType.HANDSHAKE) {
                throw new CanalClientException("expect handshake but found other type.");
            } else {
                Handshake handshake = Handshake.parseFrom(p.getBody());
                this.supportedCompressions.addAll(handshake.getSupportedCompressionsList());
                ClientAuth ca = ClientAuth.newBuilder().setUsername(this.username != null ? this.username : "").setNetReadTimeout(this.soTimeout).setNetWriteTimeout(this.soTimeout).build();
                this.writeWithHeader(this.channel, Packet.newBuilder().setType(PacketType.CLIENTAUTHENTICATION).setBody(ca.toByteString()).build().toByteArray());
                Packet ack = Packet.parseFrom(this.readNextPacket(this.channel));
                if (ack.getType() != PacketType.ACK) {
                    throw new CanalClientException("unexpected packet type when ack is expected");
                } else {
                    Ack ackBody = Ack.parseFrom(ack.getBody());
                    if (ackBody.getErrorCode() > 0) {
                        throw new CanalClientException("something goes wrong when doing authentication: " + ackBody.getErrorMessage());
                    } else {
                        this.connected = true;
                        return new InetSocketAddress(this.channel.socket().getLocalAddress(), this.channel.socket().getLocalPort());
                    }
                }
            }
        } catch (IOException var7) {
            throw new CanalClientException(var7);
        }
    }

    private synchronized void doDisconnnect() throws CanalClientException {
        this.mutex.set(true);
        if (this.channel != null) {
            try {
                this.channel.close();
            } catch (IOException var2) {
                logger.warn("exception on closing channel:{} \n {}", this.channel, var2);
            }

            this.channel = null;
        }

    }

    public void subscribe() throws CanalClientException {
        this.subscribe("");
    }

    public void subscribe(String filter) throws CanalClientException {
        this.waitClientRunning();

        try {
            this.writeWithHeader(this.channel, Packet.newBuilder().setType(PacketType.SUBSCRIPTION).setBody(Sub.newBuilder().setDestination(this.clientIdentity.getDestination()).setClientId(String.valueOf(this.clientIdentity.getClientId())).setFilter(filter != null ? filter : "").build().toByteString()).build().toByteArray());
            Packet p = Packet.parseFrom(this.readNextPacket(this.channel));
            Ack ack = Ack.parseFrom(p.getBody());
            if (ack.getErrorCode() > 0) {
                throw new CanalClientException("failed to subscribe with reason: " + ack.getErrorMessage());
            } else {
                this.clientIdentity.setFilter(filter);
            }
        } catch (IOException var4) {
            throw new CanalClientException(var4);
        }
    }

    public void unsubscribe() throws CanalClientException {
        this.waitClientRunning();

        try {
            this.writeWithHeader(this.channel, Packet.newBuilder().setType(PacketType.UNSUBSCRIPTION).setBody(Unsub.newBuilder().setDestination(this.clientIdentity.getDestination()).setClientId(String.valueOf(this.clientIdentity.getClientId())).build().toByteString()).build().toByteArray());
            Packet p = Packet.parseFrom(this.readNextPacket(this.channel));
            Ack ack = Ack.parseFrom(p.getBody());
            if (ack.getErrorCode() > 0) {
                throw new CanalClientException("failed to unSubscribe with reason: " + ack.getErrorMessage());
            }
        } catch (IOException var3) {
            throw new CanalClientException(var3);
        }
    }

    public Message get(int batchSize) throws CanalClientException {
        return this.get(batchSize, (Long)null, (TimeUnit)null);
    }

    public Message get(int batchSize, Long timeout, TimeUnit unit) throws CanalClientException {
        Message message = this.getWithoutAck(batchSize, timeout, unit);
        this.ack(message.getId());
        return message;
    }

    public Message getWithoutAck(int batchSize) throws CanalClientException {
        return this.getWithoutAck(batchSize, (Long)null, (TimeUnit)null);
    }

    public Message getWithoutAck(int batchSize, Long timeout, TimeUnit unit) throws CanalClientException {
        this.waitClientRunning();

        try {
            int size = batchSize <= 0 ? 1000 : batchSize;
            long time = timeout != null && timeout >= 0L ? timeout : -1L;
            if (unit == null) {
                unit = TimeUnit.MILLISECONDS;
            }

            this.writeWithHeader(this.channel, Packet.newBuilder().setType(PacketType.GET).setBody(Get.newBuilder().setAutoAck(false).setDestination(this.clientIdentity.getDestination()).setClientId(String.valueOf(this.clientIdentity.getClientId())).setFetchSize(size).setTimeout(time).setUnit(unit.ordinal()).build().toByteString()).build().toByteArray());
            return this.receiveMessages();
        } catch (IOException var7) {
            throw new CanalClientException(var7);
        }
    }

    private Message receiveMessages() throws InvalidProtocolBufferException, IOException {
        Packet p = Packet.parseFrom(this.readNextPacket(this.channel));
        switch(p.getType()) {
            case MESSAGES:
                if (!p.getCompression().equals(Compression.NONE)) {
                    throw new CanalClientException("compression is not supported in this connector");
                }

                Messages messages = Messages.parseFrom(p.getBody());
                Message result = new Message(messages.getBatchId());
                Iterator var4 = messages.getMessagesList().iterator();

                while(var4.hasNext()) {
                    ByteString byteString = (ByteString)var4.next();
                    result.addEntry(Entry.parseFrom(byteString));
                }

                return result;
            case ACK:
                Ack ack = Ack.parseFrom(p.getBody());
                throw new CanalClientException("something goes wrong with reason: " + ack.getErrorMessage());
            default:
                throw new CanalClientException("unexpected packet type: " + p.getType());
        }
    }

    public void ack(long batchId) throws CanalClientException {
        this.waitClientRunning();
        ClientAck ca = ClientAck.newBuilder().setDestination(this.clientIdentity.getDestination()).setClientId(String.valueOf(this.clientIdentity.getClientId())).setBatchId(batchId).build();

        try {
            this.writeWithHeader(this.channel, Packet.newBuilder().setType(PacketType.CLIENTACK).setBody(ca.toByteString()).build().toByteArray());
        } catch (IOException var5) {
            throw new CanalClientException(var5);
        }
    }

    public void rollback(long batchId) throws CanalClientException {
        this.waitClientRunning();
        ClientRollback ca = ClientRollback.newBuilder().setDestination(this.clientIdentity.getDestination()).setClientId(String.valueOf(this.clientIdentity.getClientId())).setBatchId(batchId).build();

        try {
            this.writeWithHeader(this.channel, Packet.newBuilder().setType(PacketType.CLIENTROLLBACK).setBody(ca.toByteString()).build().toByteArray());
        } catch (IOException var5) {
            throw new CanalClientException(var5);
        }
    }

    public void rollback() throws CanalClientException {
        this.waitClientRunning();
        this.rollback(0L);
    }

    private void writeWithHeader(SocketChannel channel, byte[] body) throws IOException {
        Object var3 = this.writeDataLock;
        synchronized(this.writeDataLock) {
            this.writeHeader.clear();
            this.writeHeader.putInt(body.length);
            this.writeHeader.flip();
            channel.write(this.writeHeader);
            channel.write(ByteBuffer.wrap(body));
        }
    }

    private byte[] readNextPacket(SocketChannel channel) throws IOException {
        Object var2 = this.readDataLock;
        synchronized(this.readDataLock) {
            this.readHeader.clear();
            this.read(channel, this.readHeader);
            int bodyLen = this.readHeader.getInt(0);
            ByteBuffer bodyBuf = ByteBuffer.allocate(bodyLen).order(ByteOrder.BIG_ENDIAN);
            this.read(channel, bodyBuf);
            return bodyBuf.array();
        }
    }

    private void read(SocketChannel channel, ByteBuffer buffer) throws IOException {
        while(true) {
            if (buffer.hasRemaining()) {
                int r = channel.read(buffer);
                if (r != -1) {
                    continue;
                }

                throw new IOException("end of stream when reading header");
            }

            return;
        }
    }

    private synchronized void initClientRunningMonitor(ClientIdentity clientIdentity) {
        if (this.zkClientx != null && clientIdentity != null && this.runningMonitor == null) {
            ClientRunningData clientData = new ClientRunningData();
            clientData.setClientId(clientIdentity.getClientId());
            clientData.setAddress(AddressUtils.getHostIp());
            this.runningMonitor = new StreamClientRunningMonitor();
            this.runningMonitor.setDestination(clientIdentity.getDestination());
            this.runningMonitor.setZkClient(this.zkClientx);
            this.runningMonitor.setClientData(clientData);
            this.runningMonitor.setListener(new ClientRunningListener() {
                public InetSocketAddress processActiveEnter() {
                    InetSocketAddress address = StreamSimpleCanalConnector.this.doConnect();
                    StreamSimpleCanalConnector.this.mutex.set(true);
                    if (StreamSimpleCanalConnector.this.filter != null) {
                        StreamSimpleCanalConnector.this.subscribe(StreamSimpleCanalConnector.this.filter);
                    }

                    if (StreamSimpleCanalConnector.this.rollbackOnConnect) {
                        StreamSimpleCanalConnector.this.rollback();
                    }

                    return address;
                }

                public void processActiveExit() {
                    StreamSimpleCanalConnector.this.mutex.set(false);
                    StreamSimpleCanalConnector.this.doDisconnnect();
                }
            });
        }

    }

    private void waitClientRunning() {
        try {
            if (this.zkClientx != null) {
                if (!this.connected) {
                    throw new CanalClientException("should connect first");
                }

                this.mutex.get();
            }

        } catch (InterruptedException var2) {
            Thread.currentThread().interrupt();
            throw new CanalClientException(var2);
        }
    }

    public boolean checkValid() {
        return this.zkClientx != null ? this.mutex.state() : true;
    }

    public SocketAddress getNextAddress() {
        return null;
    }

    public SocketAddress getAddress() {
        return this.address;
    }

    public String getUsername() {
        return this.username;
    }

    public String getPassword() {
        return this.password;
    }

    public int getSoTimeout() {
        return this.soTimeout;
    }

    public void setSoTimeout(int soTimeout) {
        this.soTimeout = soTimeout;
    }

    public void setZkClientx(ZkClientx zkClientx) {
        this.zkClientx = zkClientx;
        this.initClientRunningMonitor(this.clientIdentity);
    }

    public void setRollbackOnConnect(boolean rollbackOnConnect) {
        this.rollbackOnConnect = rollbackOnConnect;
    }

    public void setRollbackOnDisConnect(boolean rollbackOnDisConnect) {
        this.rollbackOnDisConnect = rollbackOnDisConnect;
    }

    public void setFilter(String filter) {
        this.filter = filter;
    }
}
