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

package org.apache.spark.rdd

import scala.reflect.{ClassTag}

import org.apache.spark.{Partition, TaskContext}

import java.util.{Properties, Random}

import scala.collection.{mutable, Map}
import scala.collection.mutable.ArrayBuffer
import scala.reflect.{classTag, ClassTag}

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus
import org.apache.hadoop.io.BytesWritable
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.TextOutputFormat

import org.apache.spark._
import org.apache.spark.Partitioner._
import org.apache.spark.SparkContext._
import org.apache.spark.annotation.{DeveloperApi, Experimental}
import org.apache.spark.broadcast.Broadcast

import scala.concurrent.Await
import akka.util.Timeout
import akka.pattern.ask
import org.apache.spark.rpc.RpcEndpointRef

import org.apache.spark.scheduler.local.{LocalBackend}
import org.apache.spark.executor.{CoarseGrainedExecutorBackend}
import org.apache.spark.scheduler.cluster._
import org.apache.spark.scheduler.cluster.{CoarseGrainedClusterMessage}
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages._
import org.apache.spark.scheduler.{DAGSchedulerEvent}
import org.apache.spark.scheduler._
import org.apache.spark.util.{CallSite, ClosureCleaner, MetadataCleaner, MetadataCleanerType, TimeStampedWeakValueHashMap, Utils}
import scala.concurrent.Future
import scala.util.{Success, Failure}
import scala.concurrent.duration._
import scala.collection.mutable.ArrayBuffer

//private[spark]
class FutureRDD[T: ClassTag, U:ClassTag](prev: RDD[T], tag:Int, method:String, ntasks:Int)
  extends RDD[U](prev) {

  logInfo(s"--DEBUG AOUT to creat New Future RDD created $tag $ntasks")

  this.id = tag

  override def getPartitions: Array[Partition] = firstParent[T].partitions

  override def compute(split: Partition, context: TaskContext) = null

  logInfo(s"--DEBUG New Future RDD created $tag $ntasks")

  override def collect( ) : Array[U] = {

    logInfo(s"--DEBUG New Future RDD collect $tag $ntasks")
    val driver : RpcEndpointRef = CoarseGrainedExecutorBackend.executorRef
    assert( driver != null )
    val results = new Array[Any](ntasks)
    val rhandler = (rem:Int,index:Int,res:Any) => {
      results(index) = res
      if( rem==1 ) results.synchronized {
        results.notify
      }
    }

    try {
      driver.send(RunJobMsg(tag,ntasks,(iter: Iterator[Any]) => iter.toArray,rhandler))

      val list = results.synchronized {
        results.wait()
        logInfo("--DEBUG notify RETURNED!!!")
        assert( results != null )
        results
      }
      Array.concat(list.asInstanceOf[Array[Array[U]]] : _*)
    } catch {
      case e : Exception =>
        logInfo(s"--DEBUG : FutureRDD $id collect : exception caught == "+e)
        logInfo("--DEBUG : TRACE  == "+e.getStackTraceString )
        null
    }
  }

  /**
    * Reduces the elements of this RDD using the specified commutative and
    * associative binary operator.
    */
  override def reduce(f: (U, U) => U): U = {
    // val cleanF = sc.clean(f)
    ClosureCleaner.clean(f,true)
    val reducePartition: Iterator[U] => Option[U] = iter => {
      if (iter.hasNext) {
        Some(iter.reduceLeft(f))
      } else {
        None
      }
    }
    //synchronize on this var
    val sync = AnyRef
    var jobResult: Option[U] = None

    val mergeResult = (rem: Int, index: Int, taskResult: Option[U]) => {
      if (taskResult.isDefined) {
        jobResult = jobResult match {
          case Some(value) => Some(f(value, taskResult.get))
          case None => taskResult
        }
      }
      if( rem==1 ) sync.synchronized {
        logInfo("--DEBUG-- RDD NOTIFY JOBRESULT")
        sync.notify()
      }
    }
    val driver : RpcEndpointRef = CoarseGrainedExecutorBackend.executorRef
    assert( driver != null )
    // try {
    driver.send(RunJobMsg[U,Option[U]](tag,ntasks,reducePartition,mergeResult))
    sync.synchronized {
      sync.wait()
      logInfo("--DEBUG JOBRESULT NOTIFY RETURNED")
      jobResult.getOrElse(throw new UnsupportedOperationException("empty collection"))
    }
  }
}
