/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.scheduler

import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.util.AsynchronousListenerBus

/**
 * Asynchronously passes SparkListenerEvents to registered SparkListeners.
 *
 * Until start() is called, all posted events are only buffered. Only after this listener bus
 * has started will events be actually propagated to all attached listeners. This listener bus
 * is stopped when it receives a SparkListenerShutdown event, which is posted using stop().
 */
private[spark] class LiveListenerBus
  extends AsynchronousListenerBus[SparkListener, SparkListenerEvent]("SparkListenerBus")
  with SparkListenerBus {

  private val logDroppedRate = new AtomicLong(System.currentTimeMillis() / 1000L + 2)

  override def onDropEvent(event: SparkListenerEvent): Unit = {

    val curtime = System.currentTimeMillis() / 1000L
    val last_log = logDroppedRate.get()

    if (last_log < curtime) {
      if (logDroppedRate.compareAndSet(last_log, curtime + 2)) {
        logError("Dropping SparkListenerEvent because no remaining room in event queue. " +
          "This likely means one of the SparkListeners is too slow and cannot keep up with " +
          "the rate at which tasks are being started by the scheduler.")
        logError(event.toString)
      }
    }
  }

}
