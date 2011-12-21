/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.giraph.comm;

import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.graph.BasicVertex;
import org.apache.giraph.graph.BspUtils;
import org.apache.giraph.graph.Edge;
import org.apache.giraph.graph.GiraphJob;
import org.apache.giraph.graph.MutableVertex;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.graph.VertexCombiner;
import org.apache.giraph.graph.VertexMutations;
import org.apache.giraph.graph.VertexResolver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.giraph.graph.WorkerInfo;
import org.apache.giraph.graph.partition.Partition;
import org.apache.giraph.graph.partition.PartitionOwner;
import org.apache.giraph.utils.MemoryUtils;

/*if[HADOOP_FACEBOOK]
import org.apache.hadoop.ipc.ProtocolSignature;
end[HADOOP_FACEBOOK]*/

@SuppressWarnings("rawtypes")
public abstract class BasicRPCCommunications<
        I extends WritableComparable,
        V extends Writable,
        E extends Writable,
        M extends Writable, J>
        implements CommunicationsInterface<I, V, E, M>,
        ServerInterface<I, V, E, M> {
    /** Class logger */
    private static final Logger LOG =
        Logger.getLogger(BasicRPCCommunications.class);
    /** Indicates whether in superstep preparation */
    private boolean inPrepareSuperstep = false;
    /** Local hostname */
    private final String localHostname;
    /** Name of RPC server, == myAddress.toString() */
    private final String myName;
    /** RPC server */
    private final Server server;
    /** Centralized service, needed to get vertex ranges */
    private final CentralizedServiceWorker<I, V, E, M> service;
    /** Hadoop configuration */
    protected final Configuration conf;
    /** Combiner instance, can be null */
    private final VertexCombiner<I, M> combiner;
    /** Address of RPC server */
    private final InetSocketAddress myAddress;
    /** Messages sent during the last superstep */
    private long totalMsgsSentInSuperstep = 0;
    /** Maximum messages sent per putVertexIdMessagesList RPC */
    private final int maxMessagesPerFlushPut;
    /**
     * Map of the peer connections, mapping from remote socket address to client
     * meta data
     */
    private final Map<InetSocketAddress, PeerConnection> peerConnections =
        new HashMap<InetSocketAddress, PeerConnection>();
    /**
     * Cached map of partition ids to remote socket address.  Needs to be
     * synchronized.
     */
    private final Map<Integer, InetSocketAddress> partitionIndexAddressMap =
        new HashMap<Integer, InetSocketAddress>();
    /**
     * Thread pool for message flush threads
     */
    private final ExecutorService executor;
    /**
     * Map of outbound messages, mapping from remote server to
     * destination vertex index to list of messages
     * (Synchronized between peer threads and main thread for each internal
     *  map)
     */
    private final Map<InetSocketAddress, Map<I, MsgList<M>>> outMessages =
        new HashMap<InetSocketAddress, Map<I, MsgList<M>>>();
    /**
     * Map of incoming messages, mapping from vertex index to list of messages.
     * Only accessed by the main thread (no need to synchronize).
     */
    private final Map<I, List<M>> inMessages = new HashMap<I, List<M>>();
    /**
     * Map of inbound messages, mapping from vertex index to list of messages.
     * Transferred to inMessages at beginning of a superstep.  This
     * intermediary step exists so that the combiner will run not only at the
     * client, but also at the server. Also, allows the sending of large
     * message lists during the superstep computation. (Synchronized)
     */
    private final Map<I, List<M>> transientInMessages =
        new HashMap<I, List<M>>();
    /**
     * Map of partition ids to incoming vertices from other workers.
     * (Synchronized)
     */
    private final Map<Integer, List<BasicVertex<I, V, E, M>>>
        inPartitionVertexMap =
            new HashMap<Integer, List<BasicVertex<I, V, E, M>>>();

    /**
     * Map from vertex index to all vertex mutations
     */
    private final Map<I, VertexMutations<I, V, E, M>>
        inVertexMutationsMap =
            new HashMap<I, VertexMutations<I, V, E, M>>();

    /** Maximum size of cached message list, before sending it out */
    private final int maxSize;
    /** Cached job id */
    private final String jobId;
    /** cached job token */
    private final J jobToken;
    /** maximum number of vertices sent in a single RPC */
    private static final int MAX_VERTICES_PER_RPC = 1024;

    /**
     * PeerConnection contains RPC client and accumulated messages
     * for a specific peer.
     */
    private class PeerConnection {
        /**
         * Map of outbound messages going to a particular remote server,
         * mapping from the destination vertex to a list of messages.
         * (Synchronized with itself).
         */
        private final Map<I, MsgList<M>> outMessagesPerPeer;
        /**
         * Client interface: RPC proxy for remote server, this class for local
         */
        private final CommunicationsInterface<I, V, E, M> peer;
        /** Boolean, set to false when local client (self), true otherwise */
        private final boolean isProxy;

        public PeerConnection(Map<I, MsgList<M>> m,
            CommunicationsInterface<I, V, E, M> i,
            boolean isProxy) {

            this.outMessagesPerPeer = m;
            this.peer = i;
            this.isProxy = isProxy;
        }

        public void close() {
            if (LOG.isDebugEnabled()) {
                LOG.debug("close: Done");
            }
        }

        public CommunicationsInterface<I, V, E, M> getRPCProxy() {
            return peer;
        }

        @Override
        public String toString() {
            return peer.getName() + ", proxy=" + isProxy;
        }
    }

    private class PeerFlushExecutor implements Runnable {
        private final PeerConnection peerConnection;
        private final Mapper<?, ?, ?, ?>.Context context;
        // Report on the status of this flusher if this interval was exceeded
        private static final int REPORTING_INTERVAL_MIN_MILLIS = 60000;

        PeerFlushExecutor(PeerConnection peerConnection,
                          Mapper<?, ?, ?, ?>.Context context) {
            this.peerConnection = peerConnection;
            this.context = context;
        }

        @Override
        public void run() {
            CommunicationsInterface<I, V, E, M> proxy
                = peerConnection.getRPCProxy();
            long startMillis = System.currentTimeMillis();
            long lastReportedMillis = startMillis;
            try {
                int verticesDone = 0;
                synchronized(peerConnection.outMessagesPerPeer) {
                    final int vertices =
                        peerConnection.outMessagesPerPeer.size();
                    // 1. Check for null messages and combine if possible
                    // 2. Send vertex ids and messages in bulk to the
                    //    destination servers.
                    for (Entry<I, MsgList<M>> entry :
                            peerConnection.outMessagesPerPeer.entrySet()) {
                        for (M msg : entry.getValue()) {
                            if (msg == null) {
                                throw new IllegalArgumentException(
                                    "run: Cannot put null message on " +
                                    "vertex id " + entry.getKey());
                            }
                        }
                        if (combiner != null && entry.getValue().size() > 1) {
                            M combinedMsg = combiner.combine(entry.getKey(),
                                                             entry.getValue());
                            entry.getValue().clear();
                            entry.getValue().add(combinedMsg);
                        }
                        if (entry.getValue().isEmpty()) {
                            throw new IllegalStateException(
                                "run: Impossible for no messages in " +
                                entry.getKey());
                        }
                    }
                    while (!peerConnection.outMessagesPerPeer.isEmpty()) {
                        int bulkedMessages = 0;
                        Iterator<Entry<I, MsgList<M>>> vertexIdMessagesListIt =
                            peerConnection.outMessagesPerPeer.entrySet().
                            iterator();
                        VertexIdMessagesList<I, M> vertexIdMessagesList =
                            new VertexIdMessagesList<I, M>();
                        while (vertexIdMessagesListIt.hasNext()) {
                            Entry<I, MsgList<M>> entry =
                                vertexIdMessagesListIt.next();
                            // Add this entry if the list is empty or we
                            // haven't reached the maximum number of messages
                            if (vertexIdMessagesList.isEmpty() ||
                                    ((bulkedMessages + entry.getValue().size())
                                     < maxMessagesPerFlushPut)) {
                                vertexIdMessagesList.add(
                                    new VertexIdMessages<I, M>(
                                        entry.getKey(), entry.getValue()));
                                bulkedMessages += entry.getValue().size();
                            }
                        }

                        // Clean up references to the vertex id and messages
                        for (VertexIdMessages<I, M>vertexIdMessages :
                                vertexIdMessagesList) {
                            peerConnection.outMessagesPerPeer.remove(
                                vertexIdMessages.getVertexId());
                        }

                        proxy.putVertexIdMessagesList(vertexIdMessagesList);
                        context.progress();

                        verticesDone += vertexIdMessagesList.size();
                        long curMillis = System.currentTimeMillis();
                        if ((lastReportedMillis +
                                REPORTING_INTERVAL_MIN_MILLIS) < curMillis) {
                            lastReportedMillis = curMillis;
                            if (LOG.isInfoEnabled()) {
                                float percentDone =
                                    (100f * verticesDone) /
                                    vertices;
                                float minutesUsed =
                                    (curMillis - startMillis) / 1000f / 60f;
                                float minutesRemaining =
                                    (minutesUsed * 100f / percentDone) -
                                    minutesUsed;
                                LOG.info("run: " + peerConnection + ", " +
                                         verticesDone + " out of " +
                                         vertices  +
                                         " done in " + minutesUsed +
                                         " minutes, " +
                                         percentDone + "% done, ETA " +
                                         minutesRemaining +
                                         " minutes remaining, " +
                                         MemoryUtils.getRuntimeMemoryStats());
                            }
                        }
                    }
                }

                if (LOG.isDebugEnabled()) {
                    LOG.debug("run: " + proxy.getName() +
                        ": all messages flushed");
                }
            } catch (IOException e) {
                LOG.error(e);
                if (peerConnection.isProxy) {
                    RPC.stopProxy(peerConnection.peer);
                }
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * LargeMessageFlushExecutor flushes all outgoing messages destined to some vertices.
     * This is executed when the number of messages destined to certain vertex
     * exceeds <i>maxSize</i>.
     */
    private class LargeMessageFlushExecutor implements Runnable {
        private final I destVertex;
        private final MsgList<M> outMessageList;
        private PeerConnection peerConnection;

        LargeMessageFlushExecutor(PeerConnection peerConnection, I destVertex) {
            this.peerConnection = peerConnection;
            synchronized(peerConnection.outMessagesPerPeer) {
                this.destVertex = destVertex;
                outMessageList =
                    peerConnection.outMessagesPerPeer.get(destVertex);
                peerConnection.outMessagesPerPeer.put(destVertex,
                                                      new MsgList<M>());
            }
        }

        @Override
        public void run() {
            try {
                CommunicationsInterface<I, V, E, M> proxy =
                    peerConnection.getRPCProxy();

                if (combiner != null) {
                    M combinedMsg = combiner.combine(destVertex,
                                                     outMessageList);
                    if (combinedMsg != null) {
                        proxy.putMsg(destVertex, combinedMsg);
                    }
                } else {
                    proxy.putMsgList(destVertex, outMessageList);
                }
            } catch (IOException e) {
                LOG.error(e);
                if (peerConnection.isProxy) {
                    RPC.stopProxy(peerConnection.peer);
                }
                throw new RuntimeException("run: Got IOException", e);
            } finally {
                outMessageList.clear();
            }
        }
    }

    private void submitLargeMessageSend(InetSocketAddress addr, I destVertex) {
        PeerConnection pc = peerConnections.get(addr);
        executor.execute(new LargeMessageFlushExecutor(pc, destVertex));
    }

    protected abstract J createJobToken() throws IOException;

    protected abstract Server getRPCServer(
        InetSocketAddress addr,
        int numHandlers, String jobId, J jobToken) throws IOException;

    /**
     * Only constructor.
     *
     * @param context Context for getting configuration
     * @param service Service worker to get the vertex ranges
     * @throws IOException
     * @throws UnknownHostException
     * @throws InterruptedException
     */
    public BasicRPCCommunications(Mapper<?, ?, ?, ?>.Context context,
                                  CentralizedServiceWorker<I, V, E, M> service)
            throws IOException, UnknownHostException, InterruptedException {
        this.service = service;
        this.conf = context.getConfiguration();
        this.maxSize = conf.getInt(GiraphJob.MSG_SIZE,
                                   GiraphJob.MSG_SIZE_DEFAULT);
        this.maxMessagesPerFlushPut =
            conf.getInt(GiraphJob.MAX_MESSAGES_PER_FLUSH_PUT,
                        GiraphJob.DEFAULT_MAX_MESSAGES_PER_FLUSH_PUT);
        if (BspUtils.getVertexCombinerClass(conf) == null) {
            this.combiner = null;
        } else {
            this.combiner = BspUtils.createVertexCombiner(conf);
        }

        this.localHostname = InetAddress.getLocalHost().getHostName();
        int taskId = conf.getInt("mapred.task.partition", -1);
        int numTasks = conf.getInt("mapred.map.tasks", 1);

        String bindAddress = localHostname;
        int bindPort = conf.getInt(GiraphJob.RPC_INITIAL_PORT,
                                   GiraphJob.RPC_INITIAL_PORT_DEFAULT) +
                                   taskId;

        this.myAddress = new InetSocketAddress(bindAddress, bindPort);
        int numHandlers = conf.getInt(GiraphJob.RPC_NUM_HANDLERS,
                                      GiraphJob.RPC_NUM_HANDLERS_DEFAULT);
        if (numTasks < numHandlers) {
            numHandlers = numTasks;
        }
        this.jobToken = createJobToken();
        this.jobId = context.getJobID().toString();
        this.server =
            getRPCServer(myAddress, numHandlers, this.jobId, this.jobToken);
        this.server.start();

        this.myName = myAddress.toString();

        int numWorkers = conf.getInt(GiraphJob.MAX_WORKERS, numTasks);
        // If the number of flush threads is unset, it is set to
        // the number of max workers - 1 or a minimum of 1.
        int numFlushThreads =
             Math.max(conf.getInt(GiraphJob.MSG_NUM_FLUSH_THREADS,
                                  numWorkers - 1),
                      1);
        this.executor = Executors.newFixedThreadPool(numFlushThreads);

        if (LOG.isInfoEnabled()) {
            LOG.info("BasicRPCCommunications: Started RPC " +
                     "communication server: " + myName + " with " +
                     numHandlers + " handlers and " + numFlushThreads +
                     " flush threads");
        }
    }

    @Override
    public void setup() {
        try {
            connectAllRPCProxys(this.jobId, this.jobToken);
        } catch (IOException e) {
            throw new IllegalStateException("setup: Got IOException", e);
        } catch (InterruptedException e) {
            throw new IllegalStateException("setup: Got InterruptedException",
                                            e);
        }
    }

    protected abstract CommunicationsInterface<I, V, E, M> getRPCProxy(
        final InetSocketAddress addr, String jobId, J jobToken)
        throws IOException, InterruptedException;

    /**
     * Establish connections to every RPC proxy server that will be used in
     * the upcoming messaging.  This method is idempotent.
     *
     * @param jobId Stringified job id
     * @param jobToken used for
     * @throws InterruptedException
     * @throws IOException
     */
    private void connectAllRPCProxys(String jobId, J jobToken)
            throws IOException, InterruptedException {
        final int maxTries = 5;
        for (PartitionOwner partitionOwner : service.getPartitionOwners()) {
            int tries = 0;
            while (tries < maxTries) {
                try {
                    startPeerConnectionThread(
                        partitionOwner.getWorkerInfo(), jobId, jobToken);
                    break;
                } catch (IOException e) {
                    LOG.warn("connectAllRPCProxys: Failed on attempt " +
                             tries + " of " + maxTries +
                             " to connect to " + partitionOwner.toString(), e);
                    ++tries;
                }
            }
        }
    }

    /**
     * Creates the connections to remote RPCs if any only if the inet socket
     * address doesn't already exist.
     *
     * @param workerInfo My worker info
     * @param jobId Id of the job
     * @param jobToken Required for secure Hadoop
     * @throws IOException
     * @throws InterruptedException
     */
    private void startPeerConnectionThread(WorkerInfo workerInfo,
                                           String jobId,
                                           J jobToken)
            throws IOException, InterruptedException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("startPeerConnectionThread: hostname " +
                      workerInfo.getHostname() + ", port " +
                      workerInfo.getPort());
        }
        final InetSocketAddress addr =
            new InetSocketAddress(workerInfo.getHostname(),
                                  workerInfo.getPort());
        // Cheap way to hold both the hostname and port (rather than
        // make a class)
        InetSocketAddress addrUnresolved =
            InetSocketAddress.createUnresolved(addr.getHostName(),
                                               addr.getPort());
        Map<I, MsgList<M>> outMsgMap = null;
        boolean isProxy = true;
        CommunicationsInterface<I, V, E, M> peer = this;
        synchronized(outMessages) {
            outMsgMap = outMessages.get(addrUnresolved);
            if (LOG.isDebugEnabled()) {
                LOG.debug("startPeerConnectionThread: Connecting to " +
                          workerInfo.toString() + ", addr = " + addr +
                          " if outMsgMap (" + outMsgMap + ") == null ");
            }
            if (outMsgMap != null) { // this host has already been added
                return;
            }

            if (myName.equals(addr.toString())) {
                isProxy = false;
            } else {
                peer = getRPCProxy(addr, jobId, jobToken);
            }

            outMsgMap = new HashMap<I, MsgList<M>>();
            outMessages.put(addrUnresolved, outMsgMap);
        }

        PeerConnection peerConnection =
            new PeerConnection(outMsgMap, peer, isProxy);
        peerConnections.put(addrUnresolved, peerConnection);
    }

    @Override
    public final long getProtocolVersion(String protocol, long clientVersion)
            throws IOException {
        return versionID;
    }

/*if[HADOOP_FACEBOOK]
    public ProtocolSignature getProtocolSignature(
            String protocol,
            long clientVersion,
            int clientMethodsHash) throws IOException {
        return new ProtocolSignature(versionID, null);
    }
end[HADOOP_FACEBOOK]*/

    @Override
    public void closeConnections() throws IOException {
        for(PeerConnection pc : peerConnections.values()) {
            pc.close();
        }
    }


    @Override
    public final void close() {
        LOG.info("close: shutting down RPC server");
        server.stop();
    }

    @Override
    public final void putMsg(I vertex, M msg) throws IOException {
        List<M> msgs = null;
        if (LOG.isDebugEnabled()) {
        	LOG.debug("putMsg: Adding msg " + msg + " on vertex " + vertex);
        }
        if (inPrepareSuperstep) {
            // Called by combiner (main thread) during superstep preparation
            msgs = inMessages.get(vertex);
            if (msgs == null) {
                msgs = new ArrayList<M>();
                inMessages.put(vertex, msgs);
            }
            msgs.add(msg);
        }
        else {
            synchronized(transientInMessages) {
                msgs = transientInMessages.get(vertex);
                if (msgs == null) {
                    msgs = new ArrayList<M>();
                    transientInMessages.put(vertex, msgs);
                }
            }
            synchronized(msgs) {
                msgs.add(msg);
            }
        }
    }

    @Override
    public final void putMsgList(I vertex,
                                 MsgList<M> msgList) throws IOException {
        List<M> msgs = null;
        if (LOG.isDebugEnabled()) {
        	LOG.debug("putMsgList: Adding msgList " + msgList +
        			" on vertex " + vertex);
        }
        synchronized(transientInMessages) {
            msgs = transientInMessages.get(vertex);
            if (msgs == null) {
                msgs = new ArrayList<M>(msgList.size());
                transientInMessages.put(vertex, msgs);
            }
        }
        synchronized(msgs) {
            msgs.addAll(msgList);
        }
    }

    @Override
    public final void putVertexIdMessagesList(
            VertexIdMessagesList<I, M> vertexIdMessagesList)
            throws IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("putVertexIdMessagesList: Adding msgList " +
                      vertexIdMessagesList);
        }

        List<M> messageList = null;
        for (VertexIdMessages<I, M> vertexIdMessages : vertexIdMessagesList) {
            synchronized(transientInMessages) {
                messageList =
                    transientInMessages.get(vertexIdMessages.getVertexId());
                if (messageList == null) {
                    messageList = new ArrayList<M>(
                        vertexIdMessages.getMessageList().size());
                    transientInMessages.put(
                        vertexIdMessages.getVertexId(), messageList);
                }
            }
            synchronized(messageList) {
                messageList.addAll(vertexIdMessages.getMessageList());
            }
        }
    }

    @Override
    public final void putVertexList(int partitionId,
                                    VertexList<I, V, E, M> vertexList)
            throws IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("putVertexList: On partition id " + partitionId +
                      " adding vertex list of size " + vertexList.size());
        }
        synchronized(inPartitionVertexMap) {
            if (vertexList.size() == 0) {
                return;
            }
            if (!inPartitionVertexMap.containsKey(partitionId)) {
                inPartitionVertexMap.put(partitionId,
                    new ArrayList<BasicVertex<I, V, E, M>>(vertexList));
            } else {
                List<BasicVertex<I, V, E, M>> tmpVertexList =
                    inPartitionVertexMap.get(partitionId);
                tmpVertexList.addAll(vertexList);
            }
        }
    }

    @Override
    public final void addEdge(I vertexIndex, Edge<I, E> edge) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("addEdge: Adding edge " + edge);
        }
        synchronized(inVertexMutationsMap) {
            VertexMutations<I, V, E, M> vertexMutations = null;
            if (!inVertexMutationsMap.containsKey(vertexIndex)) {
                vertexMutations = new VertexMutations<I, V, E, M>();
                inVertexMutationsMap.put(vertexIndex, vertexMutations);
            } else {
                vertexMutations = inVertexMutationsMap.get(vertexIndex);
            }
            vertexMutations.addEdge(edge);
        }
    }

    @Override
    public void removeEdge(I vertexIndex, I destinationVertexIndex) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("removeEdge: Removing edge on destination " +
                      destinationVertexIndex);
        }
        synchronized(inVertexMutationsMap) {
            VertexMutations<I, V, E, M> vertexMutations = null;
            if (!inVertexMutationsMap.containsKey(vertexIndex)) {
                vertexMutations = new VertexMutations<I, V, E, M>();
                inVertexMutationsMap.put(vertexIndex, vertexMutations);
            } else {
                vertexMutations = inVertexMutationsMap.get(vertexIndex);
            }
            vertexMutations.removeEdge(destinationVertexIndex);
        }
    }

    @Override
    public final void addVertex(MutableVertex<I, V, E, M> vertex) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("addVertex: Adding vertex " + vertex);
        }
        synchronized(inVertexMutationsMap) {
            VertexMutations<I, V, E, M> vertexMutations = null;
            if (!inVertexMutationsMap.containsKey(vertex.getVertexId())) {
                vertexMutations = new VertexMutations<I, V, E, M>();
                inVertexMutationsMap.put(vertex.getVertexId(), vertexMutations);
            } else {
                vertexMutations = inVertexMutationsMap.get(vertex.getVertexId());
            }
            vertexMutations.addVertex(vertex);
        }
    }

    @Override
    public void removeVertex(I vertexIndex) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("removeVertex: Removing vertex " + vertexIndex);
        }
        synchronized(inVertexMutationsMap) {
            VertexMutations<I, V, E, M> vertexMutations = null;
            if (!inVertexMutationsMap.containsKey(vertexIndex)) {
                vertexMutations = new VertexMutations<I, V, E, M>();
                inVertexMutationsMap.put(vertexIndex, vertexMutations);
            } else {
                vertexMutations = inVertexMutationsMap.get(vertexIndex);
            }
            vertexMutations.removeVertex();
        }
    }

    @Override
    public final void sendPartitionReq(WorkerInfo workerInfo,
                                       Partition<I, V, E, M> partition) {
        // Internally, break up the sending so that the list doesn't get too
        // big.
        VertexList<I, V, E, M> hadoopVertexList =
            new VertexList<I, V, E, M>();
        InetSocketAddress addr =
            getInetSocketAddress(workerInfo, partition.getPartitionId());
        CommunicationsInterface<I, V, E, M> rpcProxy =
            peerConnections.get(addr).getRPCProxy();

        if (LOG.isInfoEnabled()) {
            LOG.info("sendPartitionReq: Sending to " + rpcProxy.getName() +
                     " " + addr + " from " + workerInfo +
                     ", with partition " + partition);
        }
        for (BasicVertex<I, V, E, M> vertex : partition.getVertices()) {
            hadoopVertexList.add(vertex);
            if (hadoopVertexList.size() >= MAX_VERTICES_PER_RPC) {
                try {
                    rpcProxy.putVertexList(partition.getPartitionId(),
                                           hadoopVertexList);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                hadoopVertexList.clear();
            }
        }
        if (hadoopVertexList.size() > 0) {
            try {
                rpcProxy.putVertexList(partition.getPartitionId(),
                                       hadoopVertexList);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Fill the socket address cache for the worker info and its partition.
     *
     * @param workerInfo Worker information to get the socket address
     * @param partitionId
     * @return address of the vertex range server containing this vertex
     */
    private InetSocketAddress getInetSocketAddress(WorkerInfo workerInfo,
                                                   int partitionId) {
        synchronized(partitionIndexAddressMap) {
            InetSocketAddress address =
                partitionIndexAddressMap.get(partitionId);
            if (address == null) {
                address = InetSocketAddress.createUnresolved(
                    workerInfo.getHostname(),
                    workerInfo.getPort());
                partitionIndexAddressMap.put(partitionId, address);
            }

            if (address.getPort() != workerInfo.getPort() ||
                    !address.getHostName().equals(workerInfo.getHostname())) {
                throw new IllegalStateException(
                    "getInetSocketAddress: Impossible that address " +
                    address + " does not match " + workerInfo);
            }

            return address;
        }
    }

    /**
     * Fill the socket address cache for the partition owner.
     *
     * @param destVertex vertex to be sent
     * @return address of the vertex range server containing this vertex
     */
    private InetSocketAddress getInetSocketAddress(I destVertex) {
        PartitionOwner partitionOwner =
            service.getVertexPartitionOwner(destVertex);
        return getInetSocketAddress(partitionOwner.getWorkerInfo(),
                                    partitionOwner.getPartitionId());
    }

    @Override
    public final void sendMessageReq(I destVertex, M msg) {
        InetSocketAddress addr = getInetSocketAddress(destVertex);
        if (LOG.isDebugEnabled()) {
            LOG.debug("sendMessage: Send bytes (" + msg.toString() +
                      ") to " + destVertex + " with address " + addr);
        }
        ++totalMsgsSentInSuperstep;
        Map<I, MsgList<M>> msgMap = null;
        synchronized(outMessages) {
            msgMap = outMessages.get(addr);
        }
        if (msgMap == null) { // should never happen after constructor
            throw new RuntimeException(
                "sendMessage: msgMap did not exist for " + addr +
                " for vertex " + destVertex);
        }

        synchronized(msgMap) {
            MsgList<M> msgList = msgMap.get(destVertex);
            if (msgList == null) { // should only happen once
                msgList = new MsgList<M>();
                msgMap.put(destVertex, msgList);
            }
            msgList.add(msg);
            if (LOG.isDebugEnabled()) {
                LOG.debug("sendMessage: added msg=" + msg + ", size=" +
                          msgList.size());
            }
            if (msgList.size() > maxSize) {
                submitLargeMessageSend(addr, destVertex);
            }
        }
    }

    @Override
    public final void addEdgeReq(I destVertex, Edge<I, E> edge)
            throws IOException {
        InetSocketAddress addr = getInetSocketAddress(destVertex);
        if (LOG.isDebugEnabled()) {
            LOG.debug("addEdgeReq: Add edge (" + edge.toString() + ") to " +
                      destVertex + " with address " + addr);
        }
        CommunicationsInterface<I, V, E, M> rpcProxy =
            peerConnections.get(addr).getRPCProxy();
        rpcProxy.addEdge(destVertex, edge);
    }

    @Override
    public final void removeEdgeReq(I vertexIndex, I destVertexIndex)
            throws IOException {
        InetSocketAddress addr = getInetSocketAddress(vertexIndex);
        if (LOG.isDebugEnabled()) {
            LOG.debug("removeEdgeReq: remove edge (" + destVertexIndex +
                      ") from" + vertexIndex + " with address " + addr);
        }
        CommunicationsInterface<I, V, E, M> rpcProxy =
            peerConnections.get(addr).getRPCProxy();
        rpcProxy.removeEdge(vertexIndex, destVertexIndex);
    }

    @Override
    public final void addVertexReq(MutableVertex<I, V, E, M> vertex)
            throws IOException {
        InetSocketAddress addr = getInetSocketAddress(vertex.getVertexId());
        if (LOG.isDebugEnabled()) {
            LOG.debug("addVertexReq: Add vertex (" + vertex + ") " +
                      " with address " + addr);
        }
        CommunicationsInterface<I, V, E, M> rpcProxy =
            peerConnections.get(addr).getRPCProxy();
        rpcProxy.addVertex(vertex);
    }

    @Override
    public void removeVertexReq(I vertexIndex) throws IOException {
        InetSocketAddress addr =
            getInetSocketAddress(vertexIndex);
        if (LOG.isDebugEnabled()) {
            LOG.debug("removeVertexReq: Remove vertex index ("
                      + vertexIndex + ")  with address " + addr);
        }
        CommunicationsInterface<I, V, E, M> rpcProxy =
            peerConnections.get(addr).getRPCProxy();
        rpcProxy.removeVertex(vertexIndex);
    }

    @Override
    public long flush(Mapper<?, ?, ?, ?>.Context context) throws IOException {
        if (LOG.isInfoEnabled()) {
            LOG.info("flush: starting for superstep " +
                      service.getSuperstep() + " " +
                      MemoryUtils.getRuntimeMemoryStats());
        }
        for (List<M> msgList : inMessages.values()) {
            msgList.clear();
        }
        inMessages.clear();

        Collection<Future<?>> futures = new ArrayList<Future<?>>();

        // randomize peers in order to avoid hotspot on racks
        List<PeerConnection> peerList =
            new ArrayList<PeerConnection>(peerConnections.values());
        Collections.shuffle(peerList);

        for (PeerConnection pc : peerList) {
            futures.add(executor.submit(new PeerFlushExecutor(pc, context)));
        }

        // wait for all flushes
        for (Future<?> future : futures) {
            try {
                future.get();
                context.progress();
            } catch (InterruptedException e) {
                throw new IllegalStateException("flush: Got IOException", e);
            } catch (ExecutionException e) {
                throw new IllegalStateException(
                    "flush: Got ExecutionException", e);
            }
        }

        if (LOG.isInfoEnabled()) {
            LOG.info("flush: ended for superstep " +
                      service.getSuperstep() + " " +
                      MemoryUtils.getRuntimeMemoryStats());
        }

        long msgs = totalMsgsSentInSuperstep;
        totalMsgsSentInSuperstep = 0;
        return msgs;
    }

    @Override
    public void prepareSuperstep() {
        if (LOG.isInfoEnabled()) {
            LOG.info("prepareSuperstep: Superstep " +
                     service.getSuperstep() + " " +
                     MemoryUtils.getRuntimeMemoryStats());
        }
        inPrepareSuperstep = true;

        // Combine and put the transient messages into the inMessages.
        synchronized(transientInMessages) {
            for (Entry<I, List<M>> entry : transientInMessages.entrySet()) {
                if (combiner != null) {
                    try {
                        M combinedMsg = combiner.combine(entry.getKey(),
                                                         entry.getValue());
                        if (combinedMsg != null) {
                            putMsg(entry.getKey(), combinedMsg);
                        }
                    } catch (IOException e) {
                        // no actual IO -- should never happen
                        throw new RuntimeException(e);
                    }
                } else {
                    List<M> msgs = inMessages.get(entry.getKey());
                    if (msgs == null) {
                        msgs = new ArrayList<M>();
                        inMessages.put(entry.getKey(), msgs);
                    }
                    msgs.addAll(entry.getValue());
                }
                entry.getValue().clear();
            }
            transientInMessages.clear();
        }

        if (inMessages.size() > 0) {
            // Assign the messages to each destination vertex (getting rid of
            // the old ones)
            for (Partition<I, V, E, M> partition :
                    service.getPartitionMap().values()) {
                for (BasicVertex<I, V, E, M> vertex : partition.getVertices()) {
                    List<M> msgList = inMessages.get(vertex.getVertexId());
                    if (msgList != null) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("prepareSuperstep: Assigning " +
                                      msgList.size() +
                                      " mgs to vertex index " + vertex);
                        }
                        for (M msg : msgList) {
                            if (msg == null) {
                                LOG.warn("prepareSuperstep: Null message " +
                                         "in inMessages");
                            }
                        }
                        service.assignMessagesToVertex(vertex, msgList);
                        msgList.clear();
                        if (inMessages.remove(vertex.getVertexId()) == null) {
                            throw new IllegalStateException(
                                "prepareSuperstep: Impossible to not remove " +
                                vertex);
                        }
                    }
                }
            }
        }

        inPrepareSuperstep = false;

        // Resolve what happens when messages are sent to non-existent vertices
        // and vertices that have mutations.  Also make sure that the messages
        // are being sent to the correct destination
        Set<I> resolveVertexIndexSet = new TreeSet<I>();
        if (inMessages.size() > 0) {
            for (Entry<I, List<M>> entry : inMessages.entrySet()) {
                if (service.getPartition(entry.getKey()) == null) {
                    throw new IllegalStateException(
                        "prepareSuperstep: Impossible that this worker " +
                        service.getWorkerInfo() + " was sent " +
                        entry.getValue().size() + " message(s) with " +
                        "vertex id " + entry.getKey() +
                        " when it does not own this partition.  It should " +
                        "have gone to partition owner " +
                        service.getVertexPartitionOwner(entry.getKey()) +
                        ".  The partition owners are " +
                        service.getPartitionOwners());
                }
                resolveVertexIndexSet.add(entry.getKey());
            }
        }
        synchronized(inVertexMutationsMap) {
            for (I vertexIndex : inVertexMutationsMap.keySet()) {
                resolveVertexIndexSet.add(vertexIndex);
            }
        }

        // Resolve all graph mutations
        for (I vertexIndex : resolveVertexIndexSet) {
            VertexResolver<I, V, E, M> vertexResolver =
                BspUtils.createVertexResolver(
                    conf, service.getGraphMapper().getGraphState());
            BasicVertex<I, V, E, M> originalVertex =
                service.getVertex(vertexIndex);
            Iterable<M> messages = inMessages.get(vertexIndex);
            if (originalVertex != null) {
                messages = originalVertex.getMessages();
            }
            VertexMutations<I, V, E, M> vertexMutations =
                inVertexMutationsMap.get(vertexIndex);
            BasicVertex<I, V, E, M> vertex =
                vertexResolver.resolve(originalVertex,
                                       vertexMutations,
                                       messages);
            if (LOG.isDebugEnabled()) {
                LOG.debug("prepareSuperstep: Resolved vertex index " +
                          vertexIndex + " with original vertex " +
                          originalVertex + ", returned vertex " + vertex +
                          " on superstep " + service.getSuperstep() +
                          " with mutations " +
                          vertexMutations);
            }

            Partition<I, V, E, M> partition =
                service.getPartition(vertexIndex);
            if (partition == null) {
                throw new IllegalStateException(
                    "prepareSuperstep: No partition for index " + vertexIndex +
                    " in " + service.getPartitionMap() + " should have been " +
                    service.getVertexPartitionOwner(vertexIndex));
            }
            if (vertex != null) {
                ((MutableVertex<I, V, E, M>) vertex).setVertexId(vertexIndex);
                partition.putVertex((Vertex<I, V, E, M>) vertex);
            } else if (originalVertex != null) {
                partition.removeVertex(originalVertex.getVertexId());
            }
        }
        synchronized(inVertexMutationsMap) {
            inVertexMutationsMap.clear();
        }
    }

    @Override
    public void fixPartitionIdToSocketAddrMap() {
        // 1. Fix all the cached inet addresses (remove all changed entries)
        // 2. Connect to any new RPC servers
        synchronized(partitionIndexAddressMap) {
            for (PartitionOwner partitionOwner : service.getPartitionOwners()) {
                InetSocketAddress address =
                    partitionIndexAddressMap.get(
                        partitionOwner.getPartitionId());
               if (address != null &&
                       (!address.getHostName().equals(
                        partitionOwner.getWorkerInfo().getHostname()) ||
                        address.getPort() !=
                       partitionOwner.getWorkerInfo().getPort())) {
                   if (LOG.isInfoEnabled()) {
                       LOG.info("fixPartitionIdToSocketAddrMap: " +
                                "Partition owner " +
                                partitionOwner + " changed from " +
                                address);
                   }
                   partitionIndexAddressMap.remove(
                       partitionOwner.getPartitionId());
               }
            }
        }
        try {
            connectAllRPCProxys(this.jobId, this.jobToken);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String getName() {
        return myName;
    }

    @Override
    public Map<Integer, List<BasicVertex<I, V, E, M>>> getInPartitionVertexMap() {
        return inPartitionVertexMap;
    }
}
