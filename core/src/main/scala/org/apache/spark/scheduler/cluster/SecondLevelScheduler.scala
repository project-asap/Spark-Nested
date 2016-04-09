/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.spark.scheduler.cluster

import org.apache.spark.rpc._
import org.apache.spark.{ Logging, SparkEnv, SparkException, TaskState}
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages._
import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}
import java.io.NotSerializableException
import java.nio.ByteBuffer
import scala.util.control.NonFatal
import scala.collection.mutable.Queue
import java.io.{ByteArrayOutputStream, DataInputStream, DataOutputStream}
import org.apache.spark.util.{SerializableBuffer}
import java.util.{TimerTask, Timer}

// import scala.collection.mutable.ArrayBuffer

import scala.util.Random

//experimentalS distributed scheduler

class SecondLevelScheduler(val id:Int,
  override val rpcEnv: RpcEnv,
  val masterDriver: RpcEndpointRef,
  executorDataMap: HashMap[String,ExecutorData],
  addedFiles: HashMap[String, Long],
  addedJars: HashMap[String, Long]) extends ThreadSafeRpcEndpoint with Logging {

  private val closureSerializer = SparkEnv.get.closureSerializer.newInstance()
  // override protected def log = CoarseGrainedSchedulerBackend.this.log

  private var execList = new ArrayBuffer[String]()

  @volatile private var NEXECUTORS = execList.length

  private val r = new Random()

  private var taskQueue = new Queue[FutureTask]()

  logInfo(s"<<< EXP SecondLevelScheduling $this started! $NEXECUTORS")

  private val starvationTimer = new Timer(true)

  class FutureTask(val taskId: Long, val task: Task[_]){
    def toTaskDescription() = {

      val serializedTask: ByteBuffer = try {
        Task.serializeWithDependencies(task, addedFiles, addedJars, closureSerializer)
      } catch {
        case NonFatal(e) =>
          val msg = s"Failed to serialize task, not attempting to retry it."
          logError(msg, e)
          throw new Exception(e)
      }
      new TaskDescription(
        taskId = taskId, attemptNumber = 1, null, null, 0, task.jobId, serializedTask)
    }
  }

  class TaskQueue[T](){
    val buffer = new ArrayBuffer[T]()
    val index  = 0

    def +=(elem: T): Unit = {
      buffer += elem
    }

    def remove(chunksize:Int=1): Option[ArrayBuffer[T]] = {
      None
    }

  }

  def launchTasks() : Unit = {
    if(NEXECUTORS==0) {
      logInfo(s"<<< EXP launchTasks called NEXECS==$NEXECUTORS")
      return
    }

    while(taskQueue.isEmpty==false){
      val proxytask = taskQueue.dequeue()

      launchTask(proxytask)
    }

  }

  def launchTasks(taskArray: Array[FutureTask]): Unit = {
    assert(NEXECUTORS>0)
    // logInfo(s"<<<EXP executors ${execList.mkString(",")}")
    taskArray.foreach( ftask => {
      launchTask(ftask)
    })

  }

  def launchTask(ftask: FutureTask): Unit = {
      val eid = ftask.task.preferredLocations match{
        case Nil    =>
          logInfo(s"<<<DS($id) tid ${ftask.taskId} j: ${ftask.task.jobId} NO LOCATIONS FOUND")
          execList(r.nextInt(NEXECUTORS) ) //pick a random executor
        case locSeq =>
          logInfo(
            s"<<< DS($id) tid ${ftask.taskId} j: ${ftask.task.jobId} l:"+locSeq.mkString(",")
          )
          val l = r.nextInt(locSeq.length)
          locSeq(l) match {
            case ExecutorCacheTaskLocation(host,execid) =>
              execid
            case _                                      =>
              execList(r.nextInt(NEXECUTORS) ) //pick a random executor
          }
      }

      executorDataMap.get(eid) match {
        case Some(data) =>
          logInfo(s"<<< DS($id) SENDING TASK to exec ${data.executorHost}")
          val taskd = ftask.toTaskDescription()
          val sertdesc = closureSerializer.serialize[TaskDescription](taskd)
          data.executorEndpoint.send(LaunchTask(this.self,new SerializableBuffer(sertdesc) ))
        case None =>
          logInfo(s"<<<NO WAY EID($eid) is not in the executors List!!!")
          assert(false)
      }

  }

    override def receive: PartialFunction[Any,Unit] = {
      case msg@StatusUpdate(executorId, taskId, state, data) =>
        // logInfo(s"<<< SID($id) StatusUpdate fwd msg $msg to $masterDriver")
        masterDriver.send(msg)
        if (TaskState.isFinished(state)) {
          executorDataMap.get(executorId) match {
            case Some(executorInfo) =>
              launchTasks()
            case None =>
              // Ignoring the update since we don't know about the executor.
              logWarning(s"Ignored task status update ($taskId state $state) " +
                s"from unknown executor with ID $executorId")
          }
        }

      case msg@KillTask(taskId, executorId, interruptThread) =>
        logInfo(s"**2LScheduler fwd msg $msg to $masterDriver")
        masterDriver.send(msg)

      case ProxyLaunchTasks(taskset,indexes) =>
        // logInfo(s"<<<EXP SLS received the TaskSet $indexes !!!")
        assert(taskset.tasks.length == indexes.length)

        //divide the taskset into Task metadata using FutureTask helper class
        val taskds = Array.tabulate(indexes.length)( i =>
          new FutureTask(indexes(i),taskset.tasks(i))
        )

        if(NEXECUTORS==0){
          logInfo("<<< DISTS Enqueueing taskset for later use")
          taskQueue ++= taskds
        } else {
          launchTasks(taskds)
        }
      case UpdateExecData(eid,data) =>
        logInfo(s"<<< EXP Update executor $eid")
        executorDataMap(eid) = data
        execList += eid
        execList = execList.distinct //TODO optimize this!!
        NEXECUTORS = execList.length
        launchTasks()

      case msg@_ =>
        logInfo("NNNNNNNNNNOOOOOOOOOOOOOOO")
        logInfo(s"Case _ msg $msg")
        assert(false)
        // masterDriver.send(MsgWrapper(null,msg.asInstanceOf[CoarseGrainedClusterMessage]))
        // assert(false)

    }
}
