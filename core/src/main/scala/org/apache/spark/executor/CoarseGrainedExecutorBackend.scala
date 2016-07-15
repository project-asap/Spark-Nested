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

package org.apache.spark.executor

import java.net.URL
import java.nio.ByteBuffer

import org.apache.hadoop.conf.Configuration

import scala.collection.mutable
import scala.util.{Failure, Success}

import org.apache.spark.rpc._
import org.apache.spark._
import org.apache.spark.TaskState.TaskState
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.deploy.worker.WorkerWatcher
import org.apache.spark.scheduler.TaskDescription
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages._
import org.apache.spark.serializer.SerializerInstance
import org.apache.spark.util.{ThreadUtils, SignalLogger, Utils}

//extra imports
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessage
// import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages._
import scala.collection.mutable.{HashMap, Stack, Queue}
import collection.mutable
import org.apache.spark.scheduler._
import org.apache.spark.rdd._
import scala.concurrent._
import scala.concurrent.Await
import org.apache.spark.scheduler.local.{LocalBackend}
import org.apache.spark.scheduler._
import org.apache.spark.executor._
import scala.reflect.runtime.{universe=>ru}
import scala.reflect.runtime.universe._
import org.apache.spark.util.{CallSite, ClosureCleaner, MetadataCleaner, MetadataCleanerType, TimeStampedWeakValueHashMap, Utils}
import scala.concurrent.Future
import scala.util.{Success, Failure}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable.{ArrayBuffer,Queue}
import scala.reflect.{classTag, ClassTag}
//>>


private[spark] class CoarseGrainedExecutorBackend(
    override val rpcEnv: RpcEnv,
    driverUrl: String,
    executorId: String,
    hostPort: String,
    cores: Int,
    userClassPath: Seq[URL],
    env: SparkEnv)
  extends ThreadSafeRpcEndpoint with ExecutorBackend with Logging {

  var executor: Executor = null
  @volatile var driver: Option[RpcEndpointRef] = None

  var rddOpMap = new HashMap[Int, Seq[(String,Seq[AnyRef])]] //keep all the rdd operators, and after collect send them
  val nestedTaskMap = new HashMap[Long, (Int, RpcEndpointRef)] //(taskId->executorRef)
  private val EMPTY_BYTE_BUFFER = ByteBuffer.wrap(new Array[Byte](0))

  //TODO organize tuple in a class!!!!
  var runJobMap = new HashMap[Int,(Int,(Int,Int,Any) => Unit)]()

  var collectID = 0

  def newJobId() : Int = {
    collectID += 1
    collectID
  }


  // If this CoarseGrainedExecutorBackend is changed to support multiple threads, then this may need
  // to be changed so that we don't share the serializer instance across threads
  private[this] val ser: SerializerInstance = env.closureSerializer.newInstance()

  override def onStart() {
    logInfo("Connecting to driver: " + driverUrl)
    rpcEnv.asyncSetupEndpointRefByURI(driverUrl).flatMap { ref =>
      // This is a very fast action so we can use "ThreadUtils.sameThread"
      driver = Some(ref)
      ref.ask[RegisterExecutorResponse](
        RegisterExecutor(executorId, self, hostPort, cores, extractLogUrls))
    }(ThreadUtils.sameThread).onComplete {
      // This is a very fast action so we can use "ThreadUtils.sameThread"
      case Success(msg) => Utils.tryLogNonFatalError {
        Option(self).foreach(_.send(msg)) // msg must be RegisterExecutorResponse
      }
      case Failure(e) => {
        logError(s"Cannot register with driver: $driverUrl", e)
        System.exit(1)
      }
    }(ThreadUtils.sameThread)
  }

  def extractLogUrls: Map[String, String] = {
    val prefix = "SPARK_LOG_URL_"
    sys.env.filterKeys(_.startsWith(prefix))
      .map(e => (e._1.substring(prefix.length).toLowerCase, e._2))
  }

  override def receive: PartialFunction[Any, Unit] = {
    case RegisteredExecutor(hostname) =>
      logInfo("Successfully registered with driver")
      executor = new Executor(executorId, hostname, env, userClassPath, isLocal = false)

    case RegisterExecutorFailed(message) =>
      logError("Slave registration failed: " + message)
      System.exit(1)

    case LaunchTask(data) =>
      if (executor == null) {
        logError("Received LaunchTask command but executor was null")
        System.exit(1)
      } else {
        val taskDesc = ser.deserialize[TaskDescription](data.value)
        logInfo("Got assigned task " + taskDesc.taskId)
        executor.launchTask(this, taskId = taskDesc.taskId, attemptNumber = taskDesc.attemptNumber,
          taskDesc.name, taskDesc.serializedTask)
      }

    case LaunchNestedTask(data,sendAddr,index) =>
      assert(executor != null)
      val ser = SparkEnv.get.closureSerializer.newInstance()
      val taskDesc = ser.deserialize[TaskDescription](data.value)
      logInfo( s"--DEBUG EID($executorId) LAUNCHNESTED ${taskDesc.taskId} ${sendAddr} ")

      nestedTaskMap += (taskDesc.taskId -> (index, sendAddr))
      executor.launchTask(this, taskDesc.taskId, attemptNumber = taskDesc.attemptNumber,
        taskDesc.name, taskDesc.serializedTask)
      // logInfo( s"--DEBUG  EID($executorId) launched NESTED task $sendAddr")

    case KillTask(taskId, _, interruptThread) =>
      if (executor == null) {
        logError("Received KillTask command but executor was null")
        System.exit(1)
      } else {
        executor.killTask(taskId, interruptThread)
      }

    case StopExecutor =>
      logInfo("Driver commanded a shutdown")
      // Cannot shutdown here because an ack may need to be sent back to the caller. So send
      // a message to self to actually do the shutdown.
      self.send(Shutdown)

    case RunJobMsg(rddid,ntasks,taskfunc,rhandler) =>
      logInfo(s"--DEBUG CGEB EID($executorId) request to run Nested Job for RDD($rddid) N($ntasks)")
      logInfo(s"TASKFUNC == ${ClassTag(taskfunc.getClass())}")
      val nextJob = newJobId()
      runJobMap.update(nextJob,(ntasks,rhandler))
      val cfunc   = taskfunc.asInstanceOf[Iterator[Any]=>Any]
      val testf   = taskfunc.asInstanceOf[Iterator[Int]=>(Int,Int)]

      val f = (iter: Iterator[Any]) => iter.toArray

      val msg     = BatchRddOperators(executorId,rddid,rddOpMap(rddid),nextJob,cfunc)//f function
      safeMsg(msg)
      rddOpMap.update(rddid,List())

    case AppendRddOperator(rddid,op,args) =>
      logInfo(s"RDD $rddid pushes another arguemnt in the list")
      rddOpMap.get(rddid) match {
        case Some(list) => rddOpMap.update(rddid,list:+(op,args))
        case None => rddOpMap.update(rddid,Seq((op,args)))
      }

    case StatusUpdateExtended(eid,collectID,state,outId,data) =>
      assert( state == TaskState.FINISHED )
      //katsogr TODO handle IndirectTaskResult .....
      val index:Int = collectID.asInstanceOf[Int] // katsogr quick fix...
      val ser = SparkEnv.get.closureSerializer.newInstance()//deserialize first

      try {
        val result = ser.deserialize[TaskResult[Any]](data.value) match {
          case directResult: DirectTaskResult[_] => directResult
          case IndirectTaskResult(blockId, size) =>
            logInfo(s"--DEBUG IndirectTaskResult ID($blockId)")
            val bm = SparkEnv.get.blockManager
            val serializedTaskResult = bm.getRemoteBytes(blockId)
            val deserializedResult = ser.deserialize[DirectTaskResult[_]](
              serializedTaskResult.get)
            bm.master.removeBlock(blockId)
            deserializedResult
        }

        logInfo(s"--DEBUG STATUSUPDATEXTENDED THIS($executorId) EID($eid) INDEX($index) OID($outId)")
        logInfo(s"--DEBUG runJobMap size(${runJobMap.size})")

        val (n,f) = runJobMap(index) //TODO getOrElse( throw new Execption() )
        assert(n>0)


        f(n,outId,result.value)
        runJobMap.update(index,(n-1,f)) //append the data decrease the counter
        if(n==1){ //last element to go
          logInfo(s"--DEBUG QUEUE->NOTIFY")
          val msg = NestingFinished(executorId)
          safeMsg(msg)
        }
      } catch {
        case cnf: ClassNotFoundException =>
          logError("--ERROR ClassNotFound")
        case ex: Exception =>
          logError("Exception while getting task result", ex)
      }


    case Shutdown =>
      executor.stop()
      stop()
      rpcEnv.shutdown()
  }

  def safeMsg(msg: CoarseGrainedClusterMessage) = driver match {
    case Some(driverRef) => driverRef.send(msg)
    case None => logWarning(s"Drop $msg because has not yet connected to driver")
  }

  override def onDisconnected(remoteAddress: RpcAddress): Unit = {
    if (driver.exists(_.address == remoteAddress)) {
      logError(s"Driver $remoteAddress disassociated! Shutting down.")
      System.exit(1)
    } else {
      logWarning(s"An unknown ($remoteAddress) driver disconnected.")
    }
  }

  override def statusUpdate(taskId: Long, state: TaskState, data: ByteBuffer) {
    logInfo( s"--DEBUG E EID($executorId) TID($taskId) $state")
    if(state == TaskState.FINISHED){
      nestedTaskMap.get(taskId.asInstanceOf[Int]) match {
        case Some((index,addr)) =>
          logInfo(s"--DEBUG sendind statusUpdate TASK($taskId) ADDR($addr) INDEX($index)")
          assert(false)
          addr.send(StatusUpdate(executorId, index, TaskState.FINISHED, data))
          val msg = StatusUpdate(executorId, taskId, TaskState.NESTED_FINISHED, EMPTY_BYTE_BUFFER)
          safeMsg(msg)
        case None =>
          val msg = StatusUpdate(executorId, taskId, state, data)
          safeMsg(msg)
      }
    }else{
      val msg = StatusUpdate(executorId, taskId, state, data)
      safeMsg(msg)
    }
  }

  def statusUpdate(taskId: Long, state: TaskState, outId: Int, data: ByteBuffer) {
    logInfo( s"--DEBUG E EID($executorId) TID($taskId) $state")

    if(state == TaskState.FINISHED){
      nestedTaskMap.get(taskId.asInstanceOf[Int]) match {
        case Some((index,addr)) =>
          logInfo(s"--DEBUG sending result from TASK $taskId to ADDR $addr INDEX $index")
          addr.send(StatusUpdate(executorId, index, state, outId, data))
          val msg = StatusUpdate(executorId, taskId, TaskState.NESTED_FINISHED, EMPTY_BYTE_BUFFER)//++empty data
          safeMsg(msg)
        case None =>
          val msg = StatusUpdate(executorId, taskId, state, data)
          safeMsg(msg)
      }
    } else {
      val msg = StatusUpdate(executorId, taskId, state, data)
      driver match {
        case Some(driverRef) => driverRef.send(msg)
        case None => logWarning(s"Drop $msg because has not yet connected to driver")
      }
    }
  }
}

private[spark] object CoarseGrainedExecutorBackend extends Logging {

  var executorRef: RpcEndpointRef = null
  //taskId to (n,results,_,_)
  var collectMap = new HashMap[Int,(Long,Array[Any],Option[Iterator[Any] => Any],Option[(Int, Any) => Unit])]()
  //katsogr TODO remove array from hash
  // var collectMap = new HashMap[Int,(Long,Option[Iterator[Any] => Any],Option[(Int, Any) => Unit])]()


  var collectID : AtomicInteger = new AtomicInteger(0)


  private def run(
      driverUrl: String,
      executorId: String,
      hostname: String,
      cores: Int,
      appId: String,
      workerUrl: Option[String],
      userClassPath: Seq[URL]) {

    SignalLogger.register(log)

    SparkHadoopUtil.get.runAsSparkUser { () =>
      // Debug code
      Utils.checkHost(hostname)

      // Bootstrap to fetch the driver's Spark properties.
      val executorConf = new SparkConf
      val port = executorConf.getInt("spark.executor.port", 0)
      val fetcher = RpcEnv.create(
        "driverPropsFetcher",
        hostname,
        port,
        executorConf,
        new SecurityManager(executorConf),
        clientMode = true)
      val driver = fetcher.setupEndpointRefByURI(driverUrl)
      val props = driver.askWithRetry[Seq[(String, String)]](RetrieveSparkProps) ++
        Seq[(String, String)](("spark.app.id", appId))
      fetcher.shutdown()

      // Create SparkEnv using properties we fetched from the driver.
      val driverConf = new SparkConf()
      for ((key, value) <- props) {
        // this is required for SSL in standalone mode
        if (SparkConf.isExecutorStartupConf(key)) {
          driverConf.setIfMissing(key, value)
        } else {
          driverConf.set(key, value)
        }
      }
      if (driverConf.contains("spark.yarn.credentials.file")) {
        logInfo("Will periodically update credentials from: " +
          driverConf.get("spark.yarn.credentials.file"))
        SparkHadoopUtil.get.startExecutorDelegationTokenRenewer(driverConf)
      }

      val env = SparkEnv.createExecutorEnv(
        driverConf, executorId, hostname, port, cores, isLocal = false)

      // SparkEnv will set spark.executor.port if the rpc env is listening for incoming
      // connections (e.g., if it's using akka). Otherwise, the executor is running in
      // client mode only, and does not accept incoming connections.
      val sparkHostPort = env.conf.getOption("spark.executor.port").map { port =>
          hostname + ":" + port
      }.orNull
      //nesting set executorRef
      CoarseGrainedExecutorBackend.executorRef =
        env.rpcEnv.setupEndpoint("Executor", new CoarseGrainedExecutorBackend(
        env.rpcEnv, driverUrl, executorId, sparkHostPort, cores, userClassPath, env))
      workerUrl.foreach { url =>
        env.rpcEnv.setupEndpoint("WorkerWatcher", new WorkerWatcher(env.rpcEnv, url))
      }
      env.rpcEnv.awaitTermination()
      SparkHadoopUtil.get.stopExecutorDelegationTokenRenewer()
    }
  }

  def main(args: Array[String]) {
    var driverUrl: String = null
    var executorId: String = null
    var hostname: String = null
    var cores: Int = 0
    var appId: String = null
    var workerUrl: Option[String] = None
    val userClassPath = new mutable.ListBuffer[URL]()

    var argv = args.toList
    while (!argv.isEmpty) {
      argv match {
        case ("--driver-url") :: value :: tail =>
          driverUrl = value
          argv = tail
        case ("--executor-id") :: value :: tail =>
          executorId = value
          argv = tail
        case ("--hostname") :: value :: tail =>
          hostname = value
          argv = tail
        case ("--cores") :: value :: tail =>
          cores = value.toInt
          argv = tail
        case ("--app-id") :: value :: tail =>
          appId = value
          argv = tail
        case ("--worker-url") :: value :: tail =>
          // Worker url is used in spark standalone mode to enforce fate-sharing with worker
          workerUrl = Some(value)
          argv = tail
        case ("--user-class-path") :: value :: tail =>
          userClassPath += new URL(value)
          argv = tail
        case Nil =>
        case tail =>
          // scalastyle:off println
          System.err.println(s"Unrecognized options: ${tail.mkString(" ")}")
          // scalastyle:on println
          printUsageAndExit()
      }
    }

    if (driverUrl == null || executorId == null || hostname == null || cores <= 0 ||
      appId == null) {
      printUsageAndExit()
    }

    run(driverUrl, executorId, hostname, cores, appId, workerUrl, userClassPath)
  }

  private def printUsageAndExit() = {
    // scalastyle:off println
    System.err.println(
      """
      |"Usage: CoarseGrainedExecutorBackend [options]
      |
      | Options are:
      |   --driver-url <driverUrl>
      |   --executor-id <executorId>
      |   --hostname <hostname>
      |   --cores <cores>
      |   --app-id <appid>
      |   --worker-url <workerUrl>
      |   --user-class-path <url>
      |""".stripMargin)
    // scalastyle:on println
    System.exit(1)
  }

}
