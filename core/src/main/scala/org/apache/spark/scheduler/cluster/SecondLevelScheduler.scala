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


  def launchTasks() : Unit = {
    if(NEXECUTORS==0) {
      logInfo(s"<<< EXP launchTasks called NEXECS==$NEXECUTORS")
      return
    }

    val proxytask = if(taskQueue.isEmpty ==false) {
      taskQueue.dequeue()
    } else {
      return
    }
    logInfo(s"<<<EXP executors $execList")

    val taskd = proxytask.toTaskDescription()

    val eid = r.nextInt(NEXECUTORS)

    executorDataMap.get(execList(eid)) match {
      case Some(data) =>
        logInfo(s"<<< EXP SENDING TASK to exec ${data.executorEndpoint}")
        val sertdesc = closureSerializer.serialize[TaskDescription](taskd)
        data.executorEndpoint.send(LaunchTask(this.self,new SerializableBuffer(sertdesc) ))
      case None =>
        logInfo(s"<<<NO WAY EID($eid) is not in the executors List!!!")
        assert(false)
    }

  }


    override def receive: PartialFunction[Any,Unit] = {
      case msg@StatusUpdate(executorId, taskId, state, data) =>
        logInfo(s"<<< SID($id) StatusUpdate fwd msg $msg to $masterDriver")
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

      case msg@BatchRddOperators(executorId, rddid, ops, jobId, taskf) =>
        assert(false,"Nesting not yet supported")
        logInfo(s"**2LScheduler fwd msg $msg to $masterDriver")
        logInfo("--DEBUG caught BatchOperators Message")
        masterDriver.send(msg)

      case msg@NestingFinished(execId) =>
        logInfo(s"**2LScheduler fwd msg $msg to $masterDriver")
        masterDriver.send(msg)

      case ProxyLaunchTasks(taskset,indexes) =>
        logInfo(s"<<<EXP SLS received the TaskSet $indexes !!!")
        assert(taskset.tasks.length == indexes.length)

        //divide the taskset into Task metadata using FutureTask helper class
        val taskds = Array.tabulate(indexes.length)( i =>
          new FutureTask(indexes(i),taskset.tasks(i))
        )

        taskQueue ++= taskds
        launchTasks()

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
