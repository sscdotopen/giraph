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

package org.apache.giraph.worker;

import org.apache.giraph.zk.ZooKeeperExt;
import org.apache.log4j.Logger;

/**
 * Class which periodically writes worker's progress to zookeeper
 */
public class WorkerProgressWriter {
  /** Class logger */
  private static final Logger LOG =
      Logger.getLogger(WorkerProgressWriter.class);
  /** How often to update worker's progress */
  private static final int WRITE_UPDATE_PERIOD_MILLISECONDS = 10 * 1000;

  /** Thread which writes worker's progress */
  private final Thread writerThread;
  /** Whether worker finished application */
  private volatile boolean finished = false;

  /**
   * Constructor, starts separate thread to periodically update worker's
   * progress
   *
   * @param myProgressPath Path where this worker's progress should be stored
   * @param zk ZooKeeperExt
   */
  public WorkerProgressWriter(final String myProgressPath,
      final ZooKeeperExt zk) {
    writerThread = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          while (!finished) {
            WorkerProgress.writeToZnode(zk, myProgressPath);
            double factor = 1 + Math.random();
            Thread.sleep((long) (WRITE_UPDATE_PERIOD_MILLISECONDS * factor));
          }
        } catch (InterruptedException e) {
          // Thread is interrupted when stop is called, we can just log this
          if (LOG.isInfoEnabled()) {
            LOG.info("run: WorkerProgressWriter interrupted");
          }
        }
      }
    });
    writerThread.start();
  }

  /**
   * Stop the thread which writes worker's progress
   */
  public void stop() {
    finished = true;
    writerThread.interrupt();
  }
}
