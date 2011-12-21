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

package org.apache.giraph.graph;

import net.iharder.Base64;
import org.apache.giraph.bsp.ApplicationState;
import org.apache.giraph.bsp.BspInputFormat;
import org.apache.giraph.bsp.CentralizedService;
import org.apache.giraph.bsp.CentralizedServiceMaster;
import org.apache.giraph.bsp.SuperstepState;
import org.apache.giraph.graph.GraphMapper.MapFunctions;
import org.apache.giraph.zk.BspEvent;
import org.apache.giraph.zk.PredicateLock;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs.Ids;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.giraph.graph.partition.MasterGraphPartitioner;
import org.apache.giraph.graph.partition.PartitionOwner;
import org.apache.giraph.graph.partition.PartitionStats;
import org.apache.giraph.graph.partition.PartitionUtils;
import org.apache.giraph.utils.WritableUtils;

/**
 * ZooKeeper-based implementation of {@link CentralizedService}.
 */
@SuppressWarnings("rawtypes")
public class BspServiceMaster<
        I extends WritableComparable,
        V extends Writable,
        E extends Writable, M extends Writable>
        extends BspService<I, V, E, M>
        implements CentralizedServiceMaster<I, V, E, M> {
    /** Class logger */
    private static final Logger LOG = Logger.getLogger(BspServiceMaster.class);
    /** Superstep counter */
    private Counter superstepCounter = null;
    /** Vertex counter */
    private Counter vertexCounter = null;
    /** Finished vertex counter */
    private Counter finishedVertexCounter = null;
    /** Edge counter */
    private Counter edgeCounter = null;
    /** Sent messages counter */
    private Counter sentMessagesCounter = null;
    /** Workers on this superstep */
    private Counter currentWorkersCounter = null;
    /** Current master task partition */
    private Counter currentMasterTaskPartitionCounter = null;
    /** Last checkpointed superstep */
    private Counter lastCheckpointedSuperstepCounter = null;
    /** Am I the master? */
    private boolean isMaster = false;
    /** Max number of workers */
    private final int maxWorkers;
    /** Min number of workers */
    private final int minWorkers;
    /** Min % responded workers */
    private final float minPercentResponded;
    /** Poll period in msecs */
    private final int msecsPollPeriod;
    /** Max number of poll attempts */
    private final int maxPollAttempts;
    /** Min number of long tails before printing */
    private final int partitionLongTailMinPrint;
    /** Last finalized checkpoint */
    private long lastCheckpointedSuperstep = -1;
    /** State of the superstep changed */
    private final BspEvent superstepStateChanged =
        new PredicateLock();
    /** Master graph partitioner */
    private final MasterGraphPartitioner<I, V, E, M> masterGraphPartitioner;
    /** All the partition stats from the last superstep */
    private final List<PartitionStats> allPartitionStatsList =
        new ArrayList<PartitionStats>();
    /** Counter group name for the Giraph statistics */
    public String GIRAPH_STATS_COUNTER_GROUP_NAME = "Giraph Stats";
    /** Aggregator writer */
    public AggregatorWriter aggregatorWriter;

    public BspServiceMaster(
            String serverPortList,
            int sessionMsecTimeout,
            Mapper<?, ?, ?, ?>.Context context,
            GraphMapper<I, V, E, M> graphMapper) {
        super(serverPortList, sessionMsecTimeout, context, graphMapper);
        registerBspEvent(superstepStateChanged);

        maxWorkers =
            getConfiguration().getInt(GiraphJob.MAX_WORKERS, -1);
        minWorkers =
            getConfiguration().getInt(GiraphJob.MIN_WORKERS, -1);
        minPercentResponded =
            getConfiguration().getFloat(GiraphJob.MIN_PERCENT_RESPONDED,
                                        100.0f);
        msecsPollPeriod =
            getConfiguration().getInt(GiraphJob.POLL_MSECS,
                                      GiraphJob.POLL_MSECS_DEFAULT);
        maxPollAttempts =
            getConfiguration().getInt(GiraphJob.POLL_ATTEMPTS,
                                      GiraphJob.POLL_ATTEMPTS_DEFAULT);
        partitionLongTailMinPrint = getConfiguration().getInt(
            GiraphJob.PARTITION_LONG_TAIL_MIN_PRINT,
            GiraphJob.PARTITION_LONG_TAIL_MIN_PRINT_DEFAULT);
        masterGraphPartitioner =
            getGraphPartitionerFactory().createMasterGraphPartitioner();
    }

    @Override
    public void setJobState(ApplicationState state,
                            long applicationAttempt,
                            long desiredSuperstep) {
        JSONObject jobState = new JSONObject();
        try {
            jobState.put(JSONOBJ_STATE_KEY, state.toString());
            jobState.put(JSONOBJ_APPLICATION_ATTEMPT_KEY, applicationAttempt);
            jobState.put(JSONOBJ_SUPERSTEP_KEY, desiredSuperstep);
        } catch (JSONException e) {
            throw new RuntimeException("setJobState: Coudn't put " +
                                       state.toString());
        }
        if (LOG.isInfoEnabled()) {
            LOG.info("setJobState: " + jobState.toString() + " on superstep " +
                     getSuperstep());
        }
        try {
            getZkExt().createExt(MASTER_JOB_STATE_PATH + "/jobState",
                                 jobState.toString().getBytes(),
                                 Ids.OPEN_ACL_UNSAFE,
                                 CreateMode.PERSISTENT_SEQUENTIAL,
                                 true);
        } catch (KeeperException.NodeExistsException e) {
            throw new IllegalStateException(
                "setJobState: Imposible that " +
                MASTER_JOB_STATE_PATH + " already exists!", e);
        } catch (KeeperException e) {
            throw new IllegalStateException(
                "setJobState: Unknown KeeperException for " +
                MASTER_JOB_STATE_PATH, e);
        } catch (InterruptedException e) {
            throw new IllegalStateException(
                "setJobState: Unknown InterruptedException for " +
                MASTER_JOB_STATE_PATH, e);
        }

        if (state == ApplicationState.FAILED) {
            failJob();
        }
    }

    /**
     * Master uses this to calculate the {@link VertexInputFormat}
     * input splits and write it to ZooKeeper.
     *
     * @param numWorkers Number of available workers
     * @throws InstantiationException
     * @throws IllegalAccessException
     * @throws IOException
     * @throws InterruptedException
     */
    private List<InputSplit> generateInputSplits(int numWorkers) {
        VertexInputFormat<I, V, E, M> vertexInputFormat =
            BspUtils.<I, V, E, M>createVertexInputFormat(getConfiguration());
        List<InputSplit> splits;
        try {
            splits = vertexInputFormat.getSplits(getContext(), numWorkers);
            float samplePercent =
                getConfiguration().getFloat(
                    GiraphJob.INPUT_SPLIT_SAMPLE_PERCENT,
                    GiraphJob.INPUT_SPLIT_SAMPLE_PERCENT_DEFAULT);
            if (samplePercent != GiraphJob.INPUT_SPLIT_SAMPLE_PERCENT_DEFAULT) {
                int lastIndex = (int) (samplePercent * splits.size() / 100f);
                List<InputSplit> sampleSplits = splits.subList(0, lastIndex);
                LOG.warn("generateInputSplits: Using sampling - Processing " +
                         "only " + sampleSplits.size() + " instead of " +
                        splits.size() + " expected splits.");
                return sampleSplits;
            } else {
                if (LOG.isInfoEnabled()) {
                    LOG.info("generateInputSplits: Got " + splits.size() +
                            " input splits for " + numWorkers + " workers");
                }
                return splits;
            }
        } catch (IOException e) {
            throw new IllegalStateException(
                "generateInputSplits: Got IOException", e);
        } catch (InterruptedException e) {
            throw new IllegalStateException(
                "generateInputSplits: Got InterruptedException", e);
        }
    }

    /**
     * When there is no salvaging this job, fail it.
     *
     * @throws IOException
     */
    private void failJob() {
        LOG.fatal("failJob: Killing job " + getJobId());
        try {
            @SuppressWarnings("deprecation")
            org.apache.hadoop.mapred.JobClient jobClient =
                new org.apache.hadoop.mapred.JobClient(
                    (org.apache.hadoop.mapred.JobConf)
                    getConfiguration());
            @SuppressWarnings("deprecation")
            org.apache.hadoop.mapred.JobID jobId =
                org.apache.hadoop.mapred.JobID.forName(getJobId());
            RunningJob job = jobClient.getJob(jobId);
            job.killJob();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Parse the {@link WorkerInfo} objects from a ZooKeeper path
     * (and children).
     *
     * @param workerInfosPath Path where all the workers are children
     * @param watch Watch or not?
     * @return List of workers in that path
     */
    private List<WorkerInfo> getWorkerInfosFromPath(String workerInfosPath,
                                                    boolean watch) {
        List<WorkerInfo> workerInfoList = new ArrayList<WorkerInfo>();
        List<String> workerInfoPathList;
        try {
            workerInfoPathList =
                getZkExt().getChildrenExt(workerInfosPath, watch, false, true);
        } catch (KeeperException e) {
            throw new IllegalStateException(
                "getWorkers: Got KeeperException", e);
        } catch (InterruptedException e) {
            throw new IllegalStateException(
                "getWorkers: Got InterruptedStateException", e);
        }
        for (String workerInfoPath : workerInfoPathList) {
            WorkerInfo workerInfo = new WorkerInfo();
            WritableUtils.readFieldsFromZnode(
                getZkExt(), workerInfoPath, true, null, workerInfo);
            workerInfoList.add(workerInfo);
        }
        return workerInfoList;
    }

    /**
     * Get the healthy and unhealthy {@link WorkerInfo} objects for
     * a superstep
     *
     * @param superstep superstep to check
     * @param healthyWorkerInfoList filled in with current data
     * @param unhealthyWorkerInfoList filled in with current data
     */
    private void getAllWorkerInfos(
            long superstep,
            List<WorkerInfo> healthyWorkerInfoList,
            List<WorkerInfo> unhealthyWorkerInfoList) {
        String healthyWorkerInfoPath =
            getWorkerInfoHealthyPath(getApplicationAttempt(), superstep);
        String unhealthyWorkerInfoPath =
            getWorkerInfoUnhealthyPath(getApplicationAttempt(), superstep);

        try {
            getZkExt().createOnceExt(healthyWorkerInfoPath,
                                     null,
                                     Ids.OPEN_ACL_UNSAFE,
                                     CreateMode.PERSISTENT,
                                     true);
        } catch (KeeperException e) {
            throw new IllegalStateException("getWorkers: KeeperException", e);
        } catch (InterruptedException e) {
            throw new IllegalStateException("getWorkers: IllegalStateException"
                                            , e);
        }

        try {
            getZkExt().createOnceExt(unhealthyWorkerInfoPath,
                                     null,
                                     Ids.OPEN_ACL_UNSAFE,
                                     CreateMode.PERSISTENT,
                                     true);
        } catch (KeeperException e) {
            throw new IllegalStateException("getWorkers: KeeperException", e);
        } catch (InterruptedException e) {
            throw new IllegalStateException("getWorkers: IllegalStateException"
                                            , e);
        }

        List<WorkerInfo> currentHealthyWorkerInfoList =
            getWorkerInfosFromPath(healthyWorkerInfoPath, true);
        List<WorkerInfo> currentUnhealthyWorkerInfoList =
            getWorkerInfosFromPath(unhealthyWorkerInfoPath, false);

        healthyWorkerInfoList.clear();
        if (currentHealthyWorkerInfoList != null) {
            for (WorkerInfo healthyWorkerInfo :
                    currentHealthyWorkerInfoList) {
                healthyWorkerInfoList.add(healthyWorkerInfo);
            }
        }

        unhealthyWorkerInfoList.clear();
        if (currentUnhealthyWorkerInfoList != null) {
            for (WorkerInfo unhealthyWorkerInfo :
                    currentUnhealthyWorkerInfoList) {
                unhealthyWorkerInfoList.add(unhealthyWorkerInfo);
            }
        }
    }

    /**
     * Check all the {@link WorkerInfo} objects to ensure that a minimum
     * number of good workers exists out of the total that have reported.
     *
     * @return List of of healthy workers such that the minimum has been
     *         met, otherwise null
     */
    private List<WorkerInfo> checkWorkers() {
        boolean failJob = true;
        int pollAttempt = 0;
        List<WorkerInfo> healthyWorkerInfoList = new ArrayList<WorkerInfo>();
        List<WorkerInfo> unhealthyWorkerInfoList = new ArrayList<WorkerInfo>();
        int totalResponses = -1;
        while (pollAttempt < maxPollAttempts) {
            getAllWorkerInfos(
                getSuperstep(), healthyWorkerInfoList, unhealthyWorkerInfoList);
            totalResponses = healthyWorkerInfoList.size() +
                unhealthyWorkerInfoList.size();
            if ((totalResponses * 100.0f / maxWorkers) >=
                    minPercentResponded) {
                failJob = false;
                break;
            }
            getContext().setStatus(getGraphMapper().getMapFunctions() + " " +
                                   "checkWorkers: Only found " +
                                   totalResponses +
                                   " responses of " + maxWorkers +
                                   " needed to start superstep " +
                                   getSuperstep());
            if (getWorkerHealthRegistrationChangedEvent().waitMsecs(
                    msecsPollPeriod)) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("checkWorkers: Got event that health " +
                              "registration changed, not using poll attempt");
                }
                getWorkerHealthRegistrationChangedEvent().reset();
                continue;
            }
            if (LOG.isInfoEnabled()) {
                LOG.info("checkWorkers: Only found " + totalResponses +
                         " responses of " + maxWorkers +
                         " needed to start superstep " +
                         getSuperstep() + ".  Sleeping for " +
                         msecsPollPeriod + " msecs and used " + pollAttempt +
                         " of " + maxPollAttempts + " attempts.");
                // Find the missing workers if there are only a few
                if ((maxWorkers - totalResponses) <=
                        partitionLongTailMinPrint) {
                    Set<Integer> partitionSet = new TreeSet<Integer>();
                    for (WorkerInfo workerInfo : healthyWorkerInfoList) {
                        partitionSet.add(workerInfo.getPartitionId());
                    }
                    for (WorkerInfo workerInfo : unhealthyWorkerInfoList) {
                        partitionSet.add(workerInfo.getPartitionId());
                    }
                    for (int i = 1; i <= maxWorkers; ++i) {
                        if (partitionSet.contains(new Integer(i))) {
                            continue;
                        } else if (i == getTaskPartition()) {
                            continue;
                        } else {
                            LOG.info("checkWorkers: No response from "+
                                     "partition " + i + " (could be master)");
                        }
                    }
                }
            }
            ++pollAttempt;
        }
        if (failJob) {
            LOG.error("checkWorkers: Did not receive enough processes in " +
                      "time (only " + totalResponses + " of " +
                      minWorkers + " required).  This occurs if you do not " +
                      "have enough map tasks available simultaneously on " +
                      "your Hadoop instance to fulfill the number of " +
                      "requested workers.");
            return null;
        }

        if (healthyWorkerInfoList.size() < minWorkers) {
            LOG.error("checkWorkers: Only " + healthyWorkerInfoList.size() +
                      " available when " + minWorkers + " are required.");
            return null;
        }

        getContext().setStatus(getGraphMapper().getMapFunctions() + " " +
            "checkWorkers: Done - Found " + totalResponses +
            " responses of " + maxWorkers + " needed to start superstep " +
            getSuperstep());

        return healthyWorkerInfoList;
    }

    @Override
    public int createInputSplits() {
        // Only the 'master' should be doing this.  Wait until the number of
        // processes that have reported health exceeds the minimum percentage.
        // If the minimum percentage is not met, fail the job.  Otherwise
        // generate the input splits
        try {
            if (getZkExt().exists(INPUT_SPLIT_PATH, false) != null) {
                LOG.info(INPUT_SPLIT_PATH +
                         " already exists, no need to create");
                return Integer.parseInt(
                    new String(
                        getZkExt().getData(INPUT_SPLIT_PATH, false, null)));
            }
        } catch (KeeperException.NoNodeException e) {
            if (LOG.isInfoEnabled()) {
                LOG.info("createInputSplits: Need to create the " +
                         "input splits at " + INPUT_SPLIT_PATH);
            }
        } catch (KeeperException e) {
            throw new IllegalStateException(
                "createInputSplits: KeeperException", e);
        } catch (InterruptedException e) {
            throw new IllegalStateException(
                "createInputSplits: IllegalStateException", e);
        }

        // When creating znodes, in case the master has already run, resume
        // where it left off.
        List<WorkerInfo> healthyWorkerInfoList = checkWorkers();
        if (healthyWorkerInfoList == null) {
            setJobState(ApplicationState.FAILED, -1, -1);
            return -1;
        }

        // Note that the input splits may only be a sample if
        // INPUT_SPLIT_SAMPLE_PERCENT is set to something other than 100
        List<InputSplit> splitList =
            generateInputSplits(healthyWorkerInfoList.size());
        if (healthyWorkerInfoList.size() > splitList.size()) {
            LOG.warn("createInputSplits: Number of inputSplits="
                     + splitList.size() + " < " +
                     healthyWorkerInfoList.size() +
                     "=number of healthy processes, " +
                     "some workers will be not used");
        }
        String inputSplitPath = null;
        for (int i = 0; i< splitList.size(); ++i) {
            try {
                ByteArrayOutputStream byteArrayOutputStream =
                    new ByteArrayOutputStream();
                DataOutput outputStream =
                    new DataOutputStream(byteArrayOutputStream);
                InputSplit inputSplit = splitList.get(i);
                Text.writeString(outputStream,
                                 inputSplit.getClass().getName());
                ((Writable) inputSplit).write(outputStream);
                inputSplitPath = INPUT_SPLIT_PATH + "/" + i;
                getZkExt().createExt(inputSplitPath,
                                     byteArrayOutputStream.toByteArray(),
                                     Ids.OPEN_ACL_UNSAFE,
                                     CreateMode.PERSISTENT,
                                     true);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("createInputSplits: Created input split " +
                              "with index " + i + " serialized as " +
                              byteArrayOutputStream.toString());
                }
            } catch (KeeperException.NodeExistsException e) {
                if (LOG.isInfoEnabled()) {
                    LOG.info("createInputSplits: Node " +
                             inputSplitPath + " already exists.");
                }
            } catch (KeeperException e) {
                throw new IllegalStateException(
                    "createInputSplits: KeeperException", e);
            } catch (InterruptedException e) {
                throw new IllegalStateException(
                    "createInputSplits: IllegalStateException", e);
            } catch (IOException e) {
                throw new IllegalStateException(
                    "createInputSplits: IOException", e);
            }
        }

        // Let workers know they can start trying to load the input splits
        try {
            getZkExt().create(INPUT_SPLITS_ALL_READY_PATH,
                        null,
                        Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT);
        } catch (KeeperException.NodeExistsException e) {
            LOG.info("createInputSplits: Node " +
                     INPUT_SPLITS_ALL_READY_PATH + " already exists.");
        } catch (KeeperException e) {
            throw new IllegalStateException(
                "createInputSplits: KeeperException", e);
        } catch (InterruptedException e) {
            throw new IllegalStateException(
                "createInputSplits: IllegalStateException", e);
        }

        return splitList.size();
    }

    /**
     * Read the finalized checkpoint file and associated metadata files for the
     * checkpoint.  Modifies the {@link PartitionOwner} objects to get the
     * checkpoint prefixes.  It is an optimization to prevent all workers from
     * searching all the files.  Also read in the aggregator data from the
     * finalized checkpoint file and setting it.
     *
     * @param superstep Checkpoint set to examine.
     * @param partitionOwners Partition owners to modify with checkpoint
     *        prefixes
     * @throws IOException
     * @throws InterruptedException
     * @throws KeeperException
     */
    private void prepareCheckpointRestart(
            long superstep,
            Collection<PartitionOwner> partitionOwners)
            throws IOException, KeeperException, InterruptedException {
        FileSystem fs = getFs();
        List<Path> validMetadataPathList = new ArrayList<Path>();
        String finalizedCheckpointPath =
            getCheckpointBasePath(superstep) + CHECKPOINT_FINALIZED_POSTFIX;
        DataInputStream finalizedStream =
            fs.open(new Path(finalizedCheckpointPath));
        int prefixFileCount = finalizedStream.readInt();
        for (int i = 0; i < prefixFileCount; ++i) {
            String metadataFilePath =
                finalizedStream.readUTF() + CHECKPOINT_METADATA_POSTFIX;
            validMetadataPathList.add(new Path(metadataFilePath));
        }

        // Set the merged aggregator data if it exists.
        int aggregatorDataSize = finalizedStream.readInt();
        if (aggregatorDataSize > 0) {
            byte [] aggregatorZkData = new byte[aggregatorDataSize];
            int actualDataRead =
                finalizedStream.read(aggregatorZkData, 0, aggregatorDataSize);
            if (actualDataRead != aggregatorDataSize) {
                throw new RuntimeException(
                    "prepareCheckpointRestart: Only read " + actualDataRead +
                    " of " + aggregatorDataSize + " aggregator bytes from " +
                    finalizedCheckpointPath);
            }
            String mergedAggregatorPath =
                getMergedAggregatorPath(getApplicationAttempt(), superstep - 1);
            if (LOG.isInfoEnabled()) {
                LOG.info("prepareCheckpointRestart: Reloading merged " +
                         "aggregator " + "data '" +
                         Arrays.toString(aggregatorZkData) +
                         "' to previous checkpoint in path " +
                         mergedAggregatorPath);
            }
            if (getZkExt().exists(mergedAggregatorPath, false) == null) {
                getZkExt().createExt(mergedAggregatorPath,
                                     aggregatorZkData,
                                     Ids.OPEN_ACL_UNSAFE,
                                     CreateMode.PERSISTENT,
                                     true);
            }
            else {
                getZkExt().setData(mergedAggregatorPath, aggregatorZkData, -1);
            }
        }
        finalizedStream.close();

        Map<Integer, PartitionOwner> idOwnerMap =
            new HashMap<Integer, PartitionOwner>();
        for (PartitionOwner partitionOwner : partitionOwners) {
            if (idOwnerMap.put(partitionOwner.getPartitionId(),
                               partitionOwner) != null) {
                throw new IllegalStateException(
                    "prepareCheckpointRestart: Duplicate partition " +
                    partitionOwner);
            }
        }
        // Reading the metadata files.  Simply assign each partition owner
        // the correct file prefix based on the partition id.
        for (Path metadataPath : validMetadataPathList) {
            String checkpointFilePrefix = metadataPath.toString();
            checkpointFilePrefix =
                checkpointFilePrefix.substring(
                0,
                checkpointFilePrefix.length() -
                CHECKPOINT_METADATA_POSTFIX.length());
            DataInputStream metadataStream = fs.open(metadataPath);
            long partitions = metadataStream.readInt();
            for (long i = 0; i < partitions; ++i) {
                long dataPos = metadataStream.readLong();
                int partitionId = metadataStream.readInt();
                PartitionOwner partitionOwner = idOwnerMap.get(partitionId);
                if (LOG.isInfoEnabled()) {
                    LOG.info("prepareSuperstepRestart: File " + metadataPath +
                              " with position " + dataPos +
                              ", partition id = " + partitionId +
                              " assigned to " + partitionOwner);
                }
                partitionOwner.setCheckpointFilesPrefix(checkpointFilePrefix);
            }
            metadataStream.close();
        }
    }

    @Override
    public void setup() {
        // Might have to manually load a checkpoint.
        // In that case, the input splits are not set, they will be faked by
        // the checkpoint files.  Each checkpoint file will be an input split
        // and the input split
        superstepCounter = getContext().getCounter(
            GIRAPH_STATS_COUNTER_GROUP_NAME, "Superstep");
        vertexCounter = getContext().getCounter(
            GIRAPH_STATS_COUNTER_GROUP_NAME, "Aggregate vertices");
        finishedVertexCounter = getContext().getCounter(
            GIRAPH_STATS_COUNTER_GROUP_NAME, "Aggregate finished vertices");
        edgeCounter = getContext().getCounter(
            GIRAPH_STATS_COUNTER_GROUP_NAME, "Aggregate edges");
        sentMessagesCounter = getContext().getCounter(
            GIRAPH_STATS_COUNTER_GROUP_NAME, "Sent messages");
        currentWorkersCounter = getContext().getCounter(
            GIRAPH_STATS_COUNTER_GROUP_NAME, "Current workers");
        currentMasterTaskPartitionCounter = getContext().getCounter(
            GIRAPH_STATS_COUNTER_GROUP_NAME, "Current master task partition");
        lastCheckpointedSuperstepCounter = getContext().getCounter(
            GIRAPH_STATS_COUNTER_GROUP_NAME, "Last checkpointed superstep");
        if (getRestartedSuperstep() != UNSET_SUPERSTEP) {
            superstepCounter.increment(getRestartedSuperstep());
        }
    }

    @Override
    public boolean becomeMaster() {
        // Create my bid to become the master, then try to become the worker
        // or return false.
        String myBid = null;
        try {
            myBid =
                getZkExt().createExt(MASTER_ELECTION_PATH +
                    "/" + getHostnamePartitionId(),
                    null,
                    Ids.OPEN_ACL_UNSAFE,
                    CreateMode.EPHEMERAL_SEQUENTIAL,
                    true);
        } catch (KeeperException e) {
            throw new IllegalStateException(
                "becomeMaster: KeeperException", e);
        } catch (InterruptedException e) {
            throw new IllegalStateException(
                "becomeMaster: IllegalStateException", e);
        }
        while (true) {
            JSONObject jobState = getJobState();
            try {
                if ((jobState != null) &&
                    ApplicationState.valueOf(
                        jobState.getString(JSONOBJ_STATE_KEY)) ==
                            ApplicationState.FINISHED) {
                    LOG.info("becomeMaster: Job is finished, " +
                             "give up trying to be the master!");
                    isMaster = false;
                    return isMaster;
                }
            } catch (JSONException e) {
                throw new IllegalStateException(
                    "becomeMaster: Couldn't get state from " + jobState, e);
            }
            try {
                List<String> masterChildArr =
                    getZkExt().getChildrenExt(
                        MASTER_ELECTION_PATH, true, true, true);
                if (LOG.isInfoEnabled()) {
                    LOG.info("becomeMaster: First child is '" +
                             masterChildArr.get(0) + "' and my bid is '" +
                             myBid + "'");
                }
                if (masterChildArr.get(0).equals(myBid)) {
                    currentMasterTaskPartitionCounter.increment(
                        getTaskPartition() -
                        currentMasterTaskPartitionCounter.getValue());
                    aggregatorWriter = 
                        BspUtils.createAggregatorWriter(getConfiguration());
                    try {
                        aggregatorWriter.initialize(getContext(),
                                                    getApplicationAttempt());
                    } catch (IOException e) {
                        throw new IllegalStateException("becomeMaster: " +
                            "Couldn't initialize aggregatorWriter", e);
                    }
                    LOG.info("becomeMaster: I am now the master!");
                    isMaster = true;
                    return isMaster;
                }
                LOG.info("becomeMaster: Waiting to become the master...");
                getMasterElectionChildrenChangedEvent().waitForever();
                getMasterElectionChildrenChangedEvent().reset();
            } catch (KeeperException e) {
                throw new IllegalStateException(
                    "becomeMaster: KeeperException", e);
            } catch (InterruptedException e) {
                throw new IllegalStateException(
                    "becomeMaster: IllegalStateException", e);
            }
        }
    }

    /**
     * Collect and aggregate the worker statistics for a particular superstep.
     *
     * @param superstep Superstep to aggregate on
     * @return Global statistics aggregated on all worker statistics
     */
    private GlobalStats aggregateWorkerStats(long superstep) {
        Class<? extends Writable> partitionStatsClass =
            masterGraphPartitioner.createPartitionStats().getClass();
        GlobalStats globalStats = new GlobalStats();
        // Get the stats from the all the worker selected nodes
        String workerFinishedPath =
            getWorkerFinishedPath(getApplicationAttempt(), superstep);
        List<String> workerFinishedPathList = null;
        try {
            workerFinishedPathList =
                getZkExt().getChildrenExt(
                    workerFinishedPath, false, false, true);
        } catch (KeeperException e) {
            throw new IllegalStateException(
                "aggregateWorkerStats: KeeperException", e);
        } catch (InterruptedException e) {
            throw new IllegalStateException(
                "aggregateWorkerStats: InterruptedException", e);
        }

        allPartitionStatsList.clear();
        for (String finishedPath : workerFinishedPathList) {
            JSONObject workerFinishedInfoObj = null;
            try {
                byte [] zkData =
                    getZkExt().getData(finishedPath, false, null);
                workerFinishedInfoObj = new JSONObject(new String(zkData));
                List<? extends Writable> writableList =
                    WritableUtils.readListFieldsFromByteArray(
                        Base64.decode(workerFinishedInfoObj.getString(
                            JSONOBJ_PARTITION_STATS_KEY)),
                        partitionStatsClass,
                        getConfiguration());
                for (Writable writable : writableList) {
                    globalStats.addPartitionStats((PartitionStats) writable);
                    globalStats.addMessageCount(
                        workerFinishedInfoObj.getLong(
                            JSONOBJ_NUM_MESSAGES_KEY));
                    allPartitionStatsList.add((PartitionStats) writable);
                }
            } catch (JSONException e) {
                throw new IllegalStateException(
                    "aggregateWorkerStats: JSONException", e);
            } catch (KeeperException e) {
                throw new IllegalStateException(
                    "aggregateWorkerStats: KeeperException", e);
            } catch (InterruptedException e) {
                throw new IllegalStateException(
                    "aggregateWorkerStats: InterruptedException", e);
            } catch (IOException e) {
                throw new IllegalStateException(
                    "aggregateWorkerStats: IOException", e);
            }
         }

        if (LOG.isInfoEnabled()) {
            LOG.info("aggregateWorkerStats: Aggregation found " + globalStats +
                     " on superstep = " + getSuperstep());
        }
        return globalStats;
    }

    /**
     * Get the aggregator values for a particular superstep,
     * aggregate and save them. Does nothing on the INPUT_SUPERSTEP.
     *
     * @param superstep superstep to check
     */
    private void collectAndProcessAggregatorValues(long superstep) {
        if (superstep == INPUT_SUPERSTEP) {
            // Nothing to collect on the input superstep
            return;
        }
        Map<String, Aggregator<? extends Writable>> aggregatorMap =
            new TreeMap<String, Aggregator<? extends Writable>>();
        String workerFinishedPath =
            getWorkerFinishedPath(getApplicationAttempt(), superstep);
        List<String> hostnameIdPathList = null;
        try {
            hostnameIdPathList =
                getZkExt().getChildrenExt(
                    workerFinishedPath, false, false, true);
        } catch (KeeperException e) {
            throw new IllegalStateException(
                "collectAndProcessAggregatorValues: KeeperException", e);
        } catch (InterruptedException e) {
            throw new IllegalStateException(
                "collectAndProcessAggregatorValues: InterruptedException", e);
        }

        for (String hostnameIdPath : hostnameIdPathList) {
            JSONObject workerFinishedInfoObj = null;
            JSONArray aggregatorArray = null;
            try {
                byte [] zkData =
                    getZkExt().getData(hostnameIdPath, false, null);
                workerFinishedInfoObj = new JSONObject(new String(zkData));
            } catch (KeeperException e) {
                throw new IllegalStateException(
                    "collectAndProcessAggregatorValues: KeeperException", e);
            } catch (InterruptedException e) {
                throw new IllegalStateException(
                    "collectAndProcessAggregatorValues: InterruptedException",
                    e);
            } catch (JSONException e) {
                throw new IllegalStateException(
                    "collectAndProcessAggregatorValues: JSONException", e);
            }
            try {
                aggregatorArray = workerFinishedInfoObj.getJSONArray(
                    JSONOBJ_AGGREGATOR_VALUE_ARRAY_KEY);
            } catch (JSONException e) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("collectAndProcessAggregatorValues: " +
                              "No aggregators" + " for " + hostnameIdPath);
                }
                continue;
            }
            for (int i = 0; i < aggregatorArray.length(); ++i) {
                try {
                    if (LOG.isInfoEnabled()) {
                        LOG.info("collectAndProcessAggregatorValues: " +
                                 "Getting aggregators from " +
                                 aggregatorArray.getJSONObject(i));
                    }
                    String aggregatorName =
                        aggregatorArray.getJSONObject(i).getString(
                            AGGREGATOR_NAME_KEY);
                    String aggregatorClassName =
                        aggregatorArray.getJSONObject(i).getString(
                            AGGREGATOR_CLASS_NAME_KEY);
                    @SuppressWarnings("unchecked")
                    Aggregator<Writable> aggregator =
                        (Aggregator<Writable>) aggregatorMap.get(aggregatorName);
                    boolean firstTime = false;
                    if (aggregator == null) {
                        @SuppressWarnings("unchecked")
                        Aggregator<Writable> aggregatorWritable =
                            (Aggregator<Writable>) getAggregator(aggregatorName);
                        aggregator = aggregatorWritable;
                        if (aggregator == null) {
                            @SuppressWarnings("unchecked")
                            Class<? extends Aggregator<Writable>> aggregatorClass =
                                (Class<? extends Aggregator<Writable>>)
                                    Class.forName(aggregatorClassName);
                            aggregator = registerAggregator(
                                aggregatorName,
                                aggregatorClass);
                        }
                        aggregatorMap.put(aggregatorName, aggregator);
                        firstTime = true;
                    }
                    Writable aggregatorValue =
                        aggregator.createAggregatedValue();
                    InputStream input =
                        new ByteArrayInputStream(
                            Base64.decode(
                                aggregatorArray.getJSONObject(i).
                                getString(AGGREGATOR_VALUE_KEY)));
                    aggregatorValue.readFields(new DataInputStream(input));
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("collectAndProcessAggregatorValues: " +
                                  "aggregator value size=" + input.available() +
                                  " for aggregator=" + aggregatorName +
                                  " value=" + aggregatorValue);
                    }
                    if (firstTime) {
                        aggregator.setAggregatedValue(aggregatorValue);
                    } else {
                        aggregator.aggregate(aggregatorValue);
                    }
                } catch (IOException e) {
                    throw new IllegalStateException(
                        "collectAndProcessAggregatorValues: " +
                        "IOException when reading aggregator data " +
                        aggregatorArray, e);
                } catch (JSONException e) {
                    throw new IllegalStateException(
                        "collectAndProcessAggregatorValues: " +
                        "JSONException when reading aggregator data " +
                        aggregatorArray, e);
                } catch (ClassNotFoundException e) {
                    throw new IllegalStateException(
                        "collectAndProcessAggregatorValues: " +
                        "ClassNotFoundException when reading aggregator data " +
                        aggregatorArray, e);
                } catch (InstantiationException e) {
                    throw new IllegalStateException(
                        "collectAndProcessAggregatorValues: " +
                        "InstantiationException when reading aggregator data " +
                        aggregatorArray, e);
                } catch (IllegalAccessException e) {
                    throw new IllegalStateException(
                        "collectAndProcessAggregatorValues: " +
                        "IOException when reading aggregator data " +
                        aggregatorArray, e);
                }
            }
        }
        if (aggregatorMap.size() > 0) {
            String mergedAggregatorPath =
                getMergedAggregatorPath(getApplicationAttempt(), superstep);
            byte [] zkData = null;
            JSONArray aggregatorArray = new JSONArray();
            for (Map.Entry<String, Aggregator<? extends Writable>> entry :
                    aggregatorMap.entrySet()) {
                try {
                    ByteArrayOutputStream outputStream =
                        new ByteArrayOutputStream();
                    DataOutput output = new DataOutputStream(outputStream);
                    entry.getValue().getAggregatedValue().write(output);

                    JSONObject aggregatorObj = new JSONObject();
                    aggregatorObj.put(AGGREGATOR_NAME_KEY,
                                      entry.getKey());
                    aggregatorObj.put(
                        AGGREGATOR_VALUE_KEY,
                        Base64.encodeBytes(outputStream.toByteArray()));
                    aggregatorArray.put(aggregatorObj);
                    if (LOG.isInfoEnabled()) {
                        LOG.info("collectAndProcessAggregatorValues: " +
                                 "Trying to add aggregatorObj " +
                                 aggregatorObj + "(" +
                                 entry.getValue().getAggregatedValue() +
                                 ") to merged aggregator path " +
                                 mergedAggregatorPath);
                    }
                } catch (IOException e) {
                    throw new IllegalStateException(
                        "collectAndProcessAggregatorValues: " +
                        "IllegalStateException", e);
                } catch (JSONException e) {
                    throw new IllegalStateException(
                        "collectAndProcessAggregatorValues: JSONException", e);
                }
            }
            try {
                zkData = aggregatorArray.toString().getBytes();
                getZkExt().createExt(mergedAggregatorPath,
                                     zkData,
                                     Ids.OPEN_ACL_UNSAFE,
                                     CreateMode.PERSISTENT,
                                     true);
            } catch (KeeperException.NodeExistsException e) {
                LOG.warn("collectAndProcessAggregatorValues: " +
                         mergedAggregatorPath+
                         " already exists!");
            } catch (KeeperException e) {
                throw new IllegalStateException(
                    "collectAndProcessAggregatorValues: KeeperException", e);
            } catch (InterruptedException e) {
                throw new IllegalStateException(
                    "collectAndProcessAggregatorValues: IllegalStateException",
                    e);
            }
            if (LOG.isInfoEnabled()) {
                LOG.info("collectAndProcessAggregatorValues: Finished " +
                         "loading " +
                         mergedAggregatorPath+ " with aggregator values " +
                         aggregatorArray);
            }
        }
    }

    /**
     * Finalize the checkpoint file prefixes by taking the chosen workers and
     * writing them to a finalized file.  Also write out the master
     * aggregated aggregator array from the previous superstep.
     *
     * @param superstep superstep to finalize
     * @param chosenWorkerList list of chosen workers that will be finalized
     * @throws IOException
     * @throws InterruptedException
     * @throws KeeperException
     */
    private void finalizeCheckpoint(
            long superstep,
            List<WorkerInfo> chosenWorkerInfoList)
            throws IOException, KeeperException, InterruptedException {
        Path finalizedCheckpointPath =
            new Path(getCheckpointBasePath(superstep) +
                     CHECKPOINT_FINALIZED_POSTFIX);
        try {
            getFs().delete(finalizedCheckpointPath, false);
        } catch (IOException e) {
            LOG.warn("finalizedValidCheckpointPrefixes: Removed old file " +
                     finalizedCheckpointPath);
        }

        // Format:
        // <number of files>
        // <used file prefix 0><used file prefix 1>...
        // <aggregator data length><aggregators as a serialized JSON byte array>
        FSDataOutputStream finalizedOutputStream =
            getFs().create(finalizedCheckpointPath);
        finalizedOutputStream.writeInt(chosenWorkerInfoList.size());
        for (WorkerInfo chosenWorkerInfo : chosenWorkerInfoList) {
            String chosenWorkerInfoPrefix =
                getCheckpointBasePath(superstep) + "." +
                chosenWorkerInfo.getHostnameId();
            finalizedOutputStream.writeUTF(chosenWorkerInfoPrefix);
        }
        String mergedAggregatorPath =
            getMergedAggregatorPath(getApplicationAttempt(), superstep - 1);
        if (getZkExt().exists(mergedAggregatorPath, false) != null) {
            byte [] aggregatorZkData =
                getZkExt().getData(mergedAggregatorPath, false, null);
            finalizedOutputStream.writeInt(aggregatorZkData.length);
            finalizedOutputStream.write(aggregatorZkData);
        }
        else {
            finalizedOutputStream.writeInt(0);
        }
        finalizedOutputStream.close();
        lastCheckpointedSuperstep = superstep;
        lastCheckpointedSuperstepCounter.increment(superstep -
            lastCheckpointedSuperstepCounter.getValue());
    }

    /**
     * Assign the partitions for this superstep.  If there are changes,
     * the workers will know how to do the exchange.  If this was a restarted
     * superstep, then make sure to provide information on where to find the
     * checkpoint file.
     *
     * @param allPartitionStatsList All partition stats
     * @param chosenWorkerInfoList All the chosen worker infos
     * @param masterGraphPartitioner Master graph partitioner
     */
    private void assignPartitionOwners(
            List<PartitionStats> allPartitionStatsList,
            List<WorkerInfo> chosenWorkerInfoList,
            MasterGraphPartitioner<I, V, E, M> masterGraphPartitioner) {
        Collection<PartitionOwner> partitionOwners;
        if (getSuperstep() == INPUT_SUPERSTEP ||
                getSuperstep() == getRestartedSuperstep()) {
            partitionOwners =
                masterGraphPartitioner.createInitialPartitionOwners(
                    chosenWorkerInfoList, maxWorkers);
            if (partitionOwners.isEmpty()) {
                throw new IllegalStateException(
                    "assignAndExchangePartitions: No partition owners set");
            }
        } else {
            partitionOwners =
                masterGraphPartitioner.generateChangedPartitionOwners(
                    allPartitionStatsList,
                    chosenWorkerInfoList,
                    maxWorkers,
                    getSuperstep());

            PartitionUtils.analyzePartitionStats(partitionOwners,
                                                 allPartitionStatsList);
        }

        // If restarted, prepare the checkpoint restart
        if (getRestartedSuperstep() == getSuperstep()) {
            try {
                prepareCheckpointRestart(getSuperstep(), partitionOwners);
            } catch (IOException e) {
                throw new IllegalStateException(
                    "assignPartitionOwners: IOException on preparing", e);
            } catch (KeeperException e) {
                throw new IllegalStateException(
                    "assignPartitionOwners: KeeperException on preparing", e);
            } catch (InterruptedException e) {
                throw new IllegalStateException(
                    "assignPartitionOwners: InteruptedException on preparing",
                    e);
            }
        }

        // There will be some exchange of partitions
        if (!partitionOwners.isEmpty()) {
            String vertexExchangePath =
                getPartitionExchangePath(getApplicationAttempt(),
                                         getSuperstep());
            try {
                getZkExt().createOnceExt(vertexExchangePath,
                                         null,
                                         Ids.OPEN_ACL_UNSAFE,
                                         CreateMode.PERSISTENT,
                                         true);
            } catch (KeeperException e) {
                throw new IllegalStateException(
                    "assignPartitionOwners: KeeperException creating " +
                    vertexExchangePath);
            } catch (InterruptedException e) {
                throw new IllegalStateException(
                    "assignPartitionOwners: InterruptedException creating " +
                    vertexExchangePath);
            }
        }

        // Workers are waiting for these assignments
        String partitionAssignmentsPath =
            getPartitionAssignmentsPath(getApplicationAttempt(),
                                        getSuperstep());
        WritableUtils.writeListToZnode(
            getZkExt(),
            partitionAssignmentsPath,
            -1,
            new ArrayList<Writable>(partitionOwners));
    }

    /**
     * Check whether the workers chosen for this superstep are still alive
     *
     * @param chosenWorkerHealthPath Path to the healthy workers in ZooKeeper
     * @param chosenWorkerList List of the healthy workers
     * @return true if they are all alive, false otherwise.
     * @throws InterruptedException
     * @throws KeeperException
     */
    private boolean superstepChosenWorkerAlive(
            String chosenWorkerInfoHealthPath,
            List<WorkerInfo> chosenWorkerInfoList)
            throws KeeperException, InterruptedException {
        List<WorkerInfo> chosenWorkerInfoHealthyList =
            getWorkerInfosFromPath(chosenWorkerInfoHealthPath, false);
        Set<WorkerInfo> chosenWorkerInfoHealthySet =
            new HashSet<WorkerInfo>(chosenWorkerInfoHealthyList);
        boolean allChosenWorkersHealthy = true;
        for (WorkerInfo chosenWorkerInfo : chosenWorkerInfoList) {
            if (!chosenWorkerInfoHealthySet.contains(chosenWorkerInfo)) {
                allChosenWorkersHealthy = false;
                LOG.error("superstepChosenWorkerAlive: Missing chosen " +
                          "worker " + chosenWorkerInfo +
                          " on superstep " + getSuperstep());
            }
        }
        return allChosenWorkersHealthy;
    }

    @Override
    public void restartFromCheckpoint(long checkpoint) {
        // Process:
        // 1. Remove all old input split data
        // 2. Increase the application attempt and set to the correct checkpoint
        // 3. Send command to all workers to restart their tasks
        try {
            getZkExt().deleteExt(INPUT_SPLIT_PATH, -1, true);
        } catch (InterruptedException e) {
            throw new RuntimeException(
                "retartFromCheckpoint: InterruptedException", e);
        } catch (KeeperException e) {
            throw new RuntimeException(
                "retartFromCheckpoint: KeeperException", e);
        }
        setApplicationAttempt(getApplicationAttempt() + 1);
        setCachedSuperstep(checkpoint);
        setRestartedSuperstep(checkpoint);
        setJobState(ApplicationState.START_SUPERSTEP,
                    getApplicationAttempt(),
                    checkpoint);
    }

    /**
     * Only get the finalized checkpoint files
     */
    public static class FinalizedCheckpointPathFilter implements PathFilter {
        @Override
        public boolean accept(Path path) {
            if (path.getName().endsWith(
                    BspService.CHECKPOINT_FINALIZED_POSTFIX)) {
                return true;
            } else {
                return false;
            }
        }
    }

    @Override
    public long getLastGoodCheckpoint() throws IOException {
        // Find the last good checkpoint if none have been written to the
        // knowledge of this master
        if (lastCheckpointedSuperstep == -1) {
            FileStatus[] fileStatusArray =
                getFs().listStatus(new Path(CHECKPOINT_BASE_PATH),
                                   new FinalizedCheckpointPathFilter());
            if (fileStatusArray == null) {
                return -1;
            }
            Arrays.sort(fileStatusArray);
            lastCheckpointedSuperstep = getCheckpoint(
                fileStatusArray[fileStatusArray.length - 1].getPath());
            if (LOG.isInfoEnabled()) {
                LOG.info("getLastGoodCheckpoint: Found last good checkpoint " +
                         lastCheckpointedSuperstep + " from " +
                         fileStatusArray[fileStatusArray.length - 1].
                         getPath().toString());
            }
        }
        return lastCheckpointedSuperstep;
    }

    /**
     * Wait for a set of workers to signal that they are done with the
     * barrier.
     *
     * @param finishedWorkerPath Path to where the workers will register their
     *        hostname and id
     * @param workerInfoList List of the workers to wait for
     * @return True if barrier was successful, false if there was a worker
     *         failure
     */
    private boolean barrierOnWorkerList(String finishedWorkerPath,
                                        List<WorkerInfo> workerInfoList,
                                        BspEvent event) {
        try {
            getZkExt().createOnceExt(finishedWorkerPath,
                                     null,
                                     Ids.OPEN_ACL_UNSAFE,
                                     CreateMode.PERSISTENT,
                                     true);
        } catch (KeeperException e) {
            throw new IllegalStateException(
                "barrierOnWorkerList: KeeperException - Couldn't create " +
                finishedWorkerPath, e);
        } catch (InterruptedException e) {
            throw new IllegalStateException(
                "barrierOnWorkerList: InterruptedException - Couldn't create " +
                finishedWorkerPath, e);
        }
        List<String> hostnameIdList =
            new ArrayList<String>(workerInfoList.size());
        for (WorkerInfo workerInfo : workerInfoList) {
            hostnameIdList.add(workerInfo.getHostnameId());
        }
        String workerInfoHealthyPath =
            getWorkerInfoHealthyPath(getApplicationAttempt(), getSuperstep());
        List<String> finishedHostnameIdList;
        long nextInfoMillis = System.currentTimeMillis();
        while (true) {
            try {
                finishedHostnameIdList =
                    getZkExt().getChildrenExt(finishedWorkerPath,
                                              true,
                                              false,
                                              false);
            } catch (KeeperException e) {
                throw new IllegalStateException(
                    "barrierOnWorkerList: KeeperException - Couldn't get " +
                    "children of " + finishedWorkerPath, e);
            } catch (InterruptedException e) {
                throw new IllegalStateException(
                    "barrierOnWorkerList: IllegalException - Couldn't get " +
                    "children of " + finishedWorkerPath, e);
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("barrierOnWorkerList: Got finished worker list = " +
                          finishedHostnameIdList + ", size = " +
                          finishedHostnameIdList.size() +
                          ", worker list = " +
                          workerInfoList + ", size = " +
                          workerInfoList.size() +
                          " from " + finishedWorkerPath);
            }

            if (LOG.isInfoEnabled() &&
                    (System.currentTimeMillis() > nextInfoMillis)) {
                nextInfoMillis = System.currentTimeMillis() + 30000;
                LOG.info("barrierOnWorkerList: " +
                         finishedHostnameIdList.size() +
                         " out of " + workerInfoList.size() +
                         " workers finished on superstep " +
                         getSuperstep() + " on path " + finishedWorkerPath);
            }
            getContext().setStatus(getGraphMapper().getMapFunctions() + " - " +
                                   finishedHostnameIdList.size() +
                                   " finished out of " +
                                   workerInfoList.size() +
                                   " on superstep " + getSuperstep());
            if (finishedHostnameIdList.containsAll(hostnameIdList)) {
                break;
            }

            // Wait for a signal or no more than 60 seconds to progress
            // or else will continue.
            event.waitMsecs(60*1000);
            event.reset();
            getContext().progress();

            // Did a worker die?
            try {
                if ((getSuperstep() > 0) &&
                        !superstepChosenWorkerAlive(
                            workerInfoHealthyPath,
                            workerInfoList)) {
                    return false;
                }
            } catch (KeeperException e) {
                throw new IllegalStateException(
                    "barrierOnWorkerList: KeeperException - " +
                    "Couldn't get " + workerInfoHealthyPath, e);
            } catch (InterruptedException e) {
                throw new IllegalStateException(
                    "barrierOnWorkerList: InterruptedException - " +
                    "Couldn't get " + workerInfoHealthyPath, e);
            }
        }

        return true;
    }


    @Override
    public SuperstepState coordinateSuperstep() throws
            KeeperException, InterruptedException {
        // 1. Get chosen workers and set up watches on them.
        // 2. Assign partitions to the workers
        //    (possibly reloading from a superstep)
        // 3. Wait for all workers to complete
        // 4. Collect and process aggregators
        // 5. Create superstep finished node
        // 6. If the checkpoint frequency is met, finalize the checkpoint
        List<WorkerInfo> chosenWorkerInfoList = checkWorkers();
        if (chosenWorkerInfoList == null) {
            LOG.fatal("coordinateSuperstep: Not enough healthy workers for " +
                      "superstep " + getSuperstep());
            setJobState(ApplicationState.FAILED, -1, -1);
        } else {
            for (WorkerInfo workerInfo : chosenWorkerInfoList) {
                String workerInfoHealthyPath =
                    getWorkerInfoHealthyPath(getApplicationAttempt(),
                                             getSuperstep()) + "/" +
                                             workerInfo.getHostnameId();
                if (getZkExt().exists(workerInfoHealthyPath, true) == null) {
                    LOG.warn("coordinateSuperstep: Chosen worker " +
                             workerInfoHealthyPath +
                             " is no longer valid, failing superstep");
                }
            }
        }

        currentWorkersCounter.increment(chosenWorkerInfoList.size() -
                                        currentWorkersCounter.getValue());
        assignPartitionOwners(allPartitionStatsList,
                              chosenWorkerInfoList,
                              masterGraphPartitioner);

        if (getSuperstep() == INPUT_SUPERSTEP) {
            // Coordinate the workers finishing sending their vertices to the
            // correct workers and signal when everything is done.
            if (!barrierOnWorkerList(INPUT_SPLIT_DONE_PATH,
                                     chosenWorkerInfoList,
                                     getInputSplitsDoneStateChangedEvent())) {
                throw new IllegalStateException(
                    "coordinateSuperstep: Worker failed during input split " +
                    "(currently not supported)");
            }
            try {
                getZkExt().create(INPUT_SPLITS_ALL_DONE_PATH,
                            null,
                            Ids.OPEN_ACL_UNSAFE,
                            CreateMode.PERSISTENT);
            } catch (KeeperException.NodeExistsException e) {
                LOG.info("coordinateInputSplits: Node " +
                         INPUT_SPLITS_ALL_DONE_PATH + " already exists.");
            } catch (KeeperException e) {
                throw new IllegalStateException(
                    "coordinateInputSplits: KeeperException", e);
            } catch (InterruptedException e) {
                throw new IllegalStateException(
                    "coordinateInputSplits: IllegalStateException", e);
            }
        }

        String finishedWorkerPath =
            getWorkerFinishedPath(getApplicationAttempt(), getSuperstep());
        if (!barrierOnWorkerList(finishedWorkerPath,
                                 chosenWorkerInfoList,
                                 getSuperstepStateChangedEvent())) {
            return SuperstepState.WORKER_FAILURE;
        }

        collectAndProcessAggregatorValues(getSuperstep());
        GlobalStats globalStats = aggregateWorkerStats(getSuperstep());

        // Let everyone know the aggregated application state through the
        // superstep finishing znode.
        String superstepFinishedNode =
            getSuperstepFinishedPath(getApplicationAttempt(), getSuperstep());
        WritableUtils.writeToZnode(
            getZkExt(), superstepFinishedNode, -1, globalStats);
        vertexCounter.increment(
            globalStats.getVertexCount() -
            vertexCounter.getValue());
        finishedVertexCounter.increment(
            globalStats.getFinishedVertexCount() -
            finishedVertexCounter.getValue());
        edgeCounter.increment(
            globalStats.getEdgeCount() -
            edgeCounter.getValue());
        sentMessagesCounter.increment(
            globalStats.getMessageCount() -
            sentMessagesCounter.getValue());

        // Finalize the valid checkpoint file prefixes and possibly
        // the aggregators.
        if (checkpointFrequencyMet(getSuperstep())) {
            try {
                finalizeCheckpoint(getSuperstep(), chosenWorkerInfoList);
            } catch (IOException e) {
                throw new IllegalStateException(
                    "coordinateSuperstep: IOException on finalizing checkpoint",
                    e);
            }
        }

        // Clean up the old supersteps (always keep this one)
        long removeableSuperstep = getSuperstep() - 1;
        if ((getConfiguration().getBoolean(
                GiraphJob.KEEP_ZOOKEEPER_DATA,
                GiraphJob.KEEP_ZOOKEEPER_DATA_DEFAULT) == false) &&
                (removeableSuperstep >= 0)) {
            String oldSuperstepPath =
                getSuperstepPath(getApplicationAttempt()) + "/" +
                (removeableSuperstep);
            try {
                if (LOG.isInfoEnabled()) {
                    LOG.info("coordinateSuperstep: Cleaning up old Superstep " +
                             oldSuperstepPath);
                }
                getZkExt().deleteExt(oldSuperstepPath,
                                     -1,
                                     true);
            } catch (KeeperException.NoNodeException e) {
                LOG.warn("coordinateBarrier: Already cleaned up " +
                         oldSuperstepPath);
            } catch (KeeperException e) {
                throw new IllegalStateException(
                    "coordinateSuperstep: KeeperException on " +
                    "finalizing checkpoint", e);
            }
        }
        incrCachedSuperstep();
        // Counter starts at zero, so no need to increment
        if (getSuperstep() > 0) {
            superstepCounter.increment(1);
        }
        SuperstepState superstepState;
        if ((globalStats.getFinishedVertexCount() ==
                globalStats.getVertexCount()) &&
                globalStats.getMessageCount() == 0) {
            superstepState = SuperstepState.ALL_SUPERSTEPS_DONE;
        } else {
            superstepState = SuperstepState.THIS_SUPERSTEP_DONE;
        }
        try {
            aggregatorWriter.writeAggregator(getAggregatorMap(),
                (superstepState == SuperstepState.ALL_SUPERSTEPS_DONE) ? 
                    AggregatorWriter.LAST_SUPERSTEP : getSuperstep());
        } catch (IOException e) {
            throw new IllegalStateException(
                "coordinateSuperstep: IOException while " +
                "writing aggregators data", e);
        }
        
        return superstepState;
    }

    /**
     * Need to clean up ZooKeeper nicely.  Make sure all the masters and workers
     * have reported ending their ZooKeeper connections.
     */
    private void cleanUpZooKeeper() {
        try {
            getZkExt().createExt(CLEANED_UP_PATH,
                                 null,
                                 Ids.OPEN_ACL_UNSAFE,
                                 CreateMode.PERSISTENT,
                                 true);
        } catch (KeeperException.NodeExistsException e) {
            if (LOG.isInfoEnabled()) {
                LOG.info("cleanUpZooKeeper: Node " + CLEANED_UP_PATH +
                " already exists, no need to create.");
            }
        } catch (KeeperException e) {
            throw new IllegalStateException(
                "cleanupZooKeeper: Got KeeperException", e);
        } catch (InterruptedException e) {
            throw new IllegalStateException(
                "cleanupZooKeeper: Got IllegalStateException", e);
        }
        // Need to wait for the number of workers and masters to complete
        int maxTasks = BspInputFormat.getMaxTasks(getConfiguration());
        if ((getGraphMapper().getMapFunctions() == MapFunctions.ALL) ||
                (getGraphMapper().getMapFunctions() ==
                    MapFunctions.ALL_EXCEPT_ZOOKEEPER)) {
            maxTasks *= 2;
        }
        List<String> cleanedUpChildrenList = null;
        while (true) {
            try {
                cleanedUpChildrenList =
                    getZkExt().getChildrenExt(
                        CLEANED_UP_PATH, true, false, true);
                if (LOG.isInfoEnabled()) {
                    LOG.info("cleanUpZooKeeper: Got " +
                             cleanedUpChildrenList.size() + " of " +
                             maxTasks  +  " desired children from " +
                             CLEANED_UP_PATH);
                }
                if (cleanedUpChildrenList.size() == maxTasks) {
                    break;
                }
                if (LOG.isInfoEnabled()) {
                    LOG.info("cleanedUpZooKeeper: Waiting for the " +
                             "children of " + CLEANED_UP_PATH +
                             " to change since only got " +
                             cleanedUpChildrenList.size() + " nodes.");
                }
            }
            catch (Exception e) {
                // We are in the cleanup phase -- just log the error
                LOG.error("cleanUpZooKeeper: Got exception, but will continue",
                          e);
                return;
            }

            getCleanedUpChildrenChangedEvent().waitForever();
            getCleanedUpChildrenChangedEvent().reset();
        }

         // At this point, all processes have acknowledged the cleanup,
         // and the master can do any final cleanup
        try {
            if (getConfiguration().getBoolean(
                GiraphJob.KEEP_ZOOKEEPER_DATA,
                GiraphJob.KEEP_ZOOKEEPER_DATA_DEFAULT) == false) {
                if (LOG.isInfoEnabled()) {
                    LOG.info("cleanupZooKeeper: Removing the following path " +
                             "and all children - " + BASE_PATH);
                }
                getZkExt().deleteExt(BASE_PATH, -1, true);
            }
        } catch (Exception e) {
            LOG.error("cleanupZooKeeper: Failed to do cleanup of " +
                      BASE_PATH, e);
        }
    }

    @Override
    public void cleanup() throws IOException {
        // All master processes should denote they are done by adding special
        // znode.  Once the number of znodes equals the number of partitions
        // for workers and masters, the master will clean up the ZooKeeper
        // znodes associated with this job.
        String cleanedUpPath = CLEANED_UP_PATH  + "/" +
            getTaskPartition() + MASTER_SUFFIX;
        try {
            String finalFinishedPath =
                getZkExt().createExt(cleanedUpPath,
                                     null,
                                     Ids.OPEN_ACL_UNSAFE,
                                     CreateMode.PERSISTENT,
                                     true);
            if (LOG.isInfoEnabled()) {
                LOG.info("cleanup: Notifying master its okay to cleanup with " +
                         finalFinishedPath);
            }
        } catch (KeeperException.NodeExistsException e) {
            if (LOG.isInfoEnabled()) {
                LOG.info("cleanup: Couldn't create finished node '" +
                         cleanedUpPath);
            }
        } catch (KeeperException e) {
            LOG.error("cleanup: Got KeeperException, continuing", e);
        } catch (InterruptedException e) {
            LOG.error("cleanup: Got InterruptedException, continuing", e);
        }

        if (isMaster) {
            cleanUpZooKeeper();
            // If desired, cleanup the checkpoint directory
            if (getConfiguration().getBoolean(
                    GiraphJob.CLEANUP_CHECKPOINTS_AFTER_SUCCESS,
                    GiraphJob.CLEANUP_CHECKPOINTS_AFTER_SUCCESS_DEFAULT)) {
                boolean success =
                    getFs().delete(new Path(CHECKPOINT_BASE_PATH), true);
                if (LOG.isInfoEnabled()) {
                    LOG.info("cleanup: Removed HDFS checkpoint directory (" +
                             CHECKPOINT_BASE_PATH + ") with return = " +
                             success + " since this job succeeded ");
                }
            }
            aggregatorWriter.close();
        }

        try {
            getZkExt().close();
        } catch (InterruptedException e) {
            // cleanup phase -- just log the error
            LOG.error("cleanup: Zookeeper failed to close", e);
        }
    }

    /**
     * Event that the master watches that denotes if a worker has done something
     * that changes the state of a superstep (either a worker completed or died)
     *
     * @return Event that denotes a superstep state change
     */
    final public BspEvent getSuperstepStateChangedEvent() {
        return superstepStateChanged;
    }

    /**
     * Should this worker failure cause the current superstep to fail?
     *
     * @param failedWorkerPath Full path to the failed worker
     */
    final private void checkHealthyWorkerFailure(String failedWorkerPath) {
        if (getSuperstepFromPath(failedWorkerPath) < getSuperstep()) {
            return;
        }

        Collection<PartitionOwner> partitionOwners =
            masterGraphPartitioner.getCurrentPartitionOwners();
        String hostnameId =
            getHealthyHostnameIdFromPath(failedWorkerPath);
        for (PartitionOwner partitionOwner : partitionOwners) {
            WorkerInfo workerInfo = partitionOwner.getWorkerInfo();
            WorkerInfo previousWorkerInfo =
                partitionOwner.getPreviousWorkerInfo();
            if (workerInfo.getHostnameId().equals(hostnameId) ||
                ((previousWorkerInfo != null) &&
                    previousWorkerInfo.getHostnameId().equals(hostnameId))) {
                LOG.warn("checkHealthyWorkerFailure: " +
                        "at least one healthy worker went down " +
                        "for superstep " + getSuperstep() + " - " +
                        hostnameId + ", will try to restart from " +
                        "checkpointed superstep " +
                        lastCheckpointedSuperstep);
                superstepStateChanged.signal();
            }
        }
    }

    @Override
    public boolean processEvent(WatchedEvent event) {
        boolean foundEvent = false;
        if (event.getPath().contains(WORKER_HEALTHY_DIR) &&
                (event.getType() == EventType.NodeDeleted)) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("processEvent: Healthy worker died (node deleted) " +
                          "in " + event.getPath());
            }
            checkHealthyWorkerFailure(event.getPath());
            superstepStateChanged.signal();
            foundEvent = true;
        } else if (event.getPath().contains(WORKER_FINISHED_DIR) &&
                event.getType() == EventType.NodeChildrenChanged) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("processEvent: Worker finished (node change) " +
                          "event - superstepStateChanged signaled");
            }
            superstepStateChanged.signal();
            foundEvent = true;
        }

        return foundEvent;
    }
}
