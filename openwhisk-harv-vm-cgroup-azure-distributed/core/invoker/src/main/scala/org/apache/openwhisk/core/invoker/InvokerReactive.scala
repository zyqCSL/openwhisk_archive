/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.openwhisk.core.invoker

import java.nio.charset.StandardCharsets
import java.time.Instant

import akka.Done
import akka.actor.{ActorRefFactory, ActorSystem, CoordinatedShutdown, Props}
import akka.event.Logging.InfoLevel
import akka.stream.ActorMaterializer
import org.apache.kafka.common.errors.RecordTooLargeException
import org.apache.openwhisk.common._
import org.apache.openwhisk.common.tracing.WhiskTracerProvider
import org.apache.openwhisk.core.connector._
import org.apache.openwhisk.core.containerpool._
import org.apache.openwhisk.core.containerpool.logging.LogStoreProvider
import org.apache.openwhisk.core.database.{UserContext, _}
import org.apache.openwhisk.core.entity._
import org.apache.openwhisk.core.{ConfigKeys, WhiskConfig}
import org.apache.openwhisk.http.Messages
import org.apache.openwhisk.spi.SpiLoader
import pureconfig._
import spray.json._

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}
import scala.io.Source
import java.nio.file.{Paths, Files} // yanqi, check file exists
// yanqi, needs to be renamed, otherwise conflit with immutable collection
import scala.collection.mutable.{Map=>MMap}

object InvokerReactive extends InvokerProvider {

  // yanqi, add cpu util & execution time & total time
  /**
   * An method for sending Active Acknowledgements (aka "active ack") messages to the load balancer. These messages
   * are either completion messages for an activation to indicate a resource slot is free, or result-forwarding
   * messages for continuations (e.g., sequences and conductor actions).
   *
   * @param TransactionId the transaction id for the activation
   * @param WhiskActivaiton is the activation result
   * @param Boolean is true iff the activation was a blocking request
   * @param ControllerInstanceId the originating controller/loadbalancer id
   * @param UUID is the UUID for the namespace owning the activation
   * @param Boolean is true this is resource free message and false if this is a result forwarding message
   * @param Double is cpu utilization of the function
   * @param Long is execution time of the function
   * @param Long is total time of the function (including cold start)
   */
  type ActiveAck = (TransactionId, WhiskActivation, Boolean, ControllerInstanceId, UUID, Boolean, 
    Double, Long, Long) => Future[Any]

  override def instance(
    config: WhiskConfig,
    instance: InvokerInstanceId,
    producer: MessageProducer,
    poolConfig: ContainerPoolConfig,
    limitsConfig: ConcurrencyLimitConfig)(implicit actorSystem: ActorSystem, logging: Logging): InvokerCore =
    new InvokerReactive(config, instance, producer, poolConfig, limitsConfig)

}

class InvokerReactive(
  config: WhiskConfig,
  instance: InvokerInstanceId,
  producer: MessageProducer,
  poolConfig: ContainerPoolConfig = loadConfigOrThrow[ContainerPoolConfig](ConfigKeys.containerPool),
  limitsConfig: ConcurrencyLimitConfig = loadConfigOrThrow[ConcurrencyLimitConfig](ConfigKeys.concurrencyLimit))(
  implicit actorSystem: ActorSystem,
  logging: Logging)
    extends InvokerCore {

  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val ec: ExecutionContext = actorSystem.dispatcher
  implicit val cfg: WhiskConfig = config

  private val logsProvider = SpiLoader.get[LogStoreProvider].instance(actorSystem)
  logging.info(this, s"LogStoreProvider: ${logsProvider.getClass}")

  /***** rsc accounting, yanqi *****/
  val resourcePath = "/hypervkvp/.kvp_pool_0"
  val cgroupCpuPath = "/sys/fs/cgroup/cpuacct/cgroup_harvest_vm/cpuacct.usage"
  val cgroupMemPath = "/sys/fs/cgroup/memory/cgroup_harvest_vm/memory.stat"
  var cgroupCpuTime: Long = 0   // in ns
  var cgroupCpuUsage: Double = 0.0 // virtual cpus
  var cgroupMemUsage: Int = 0 // in mb
  var cgroupWindowSize: Int = 5
  // list of (cpu_usage, mem_usage) tuples
  var cgroupWindow: Array[(Double, Int)] = Array.fill(cgroupWindowSize)((-1.0, -1))
  var cgroupWindowPtr: Int = 0
  var cgroupCheckTime: Long = 0 // in ms

  def get_mean_rsc_usage(): (Double, Int) = {
    var samples: Int = 0
    var sum_cpu: Double = 0
    var sum_mem: Int = 0
    var i: Int = 0
    while(i < cgroupWindowSize) {
      if(cgroupWindow(i)._1 >= 0 && cgroupWindow(i)._2 >= 0) {
        samples = samples + 1
        sum_cpu = sum_cpu + cgroupWindow(i)._1
        sum_mem = sum_mem + cgroupWindow(i)._2
      }
      i = i + 1
    }
    (sum_cpu/samples, sum_mem/samples)
  }

  def get_max_rsc_usage(): (Double, Int) = {
    var max_cpu: Double = 0
    var max_mem: Int = 0
    var i: Int = 0
    while(i < cgroupWindowSize) {
      if(cgroupWindow(i)._1 > max_cpu ) {
        max_cpu = cgroupWindow(i)._1
      }
      if(cgroupWindow(i)._2 > max_mem) {
        max_mem = cgroupWindow(i)._2
      }
      i = i + 1
    }
    (max_cpu, max_mem)
  }

  def proceed_cgroup_window_ptr() {
    cgroupWindowPtr = cgroupWindowPtr + 1
    if(cgroupWindowPtr >= cgroupWindowSize) {
      cgroupWindowPtr = 0
    }
  }

  /***** controller accounting. yanqi *****/
  class SyncIdMap() {
    var syncMap: MMap[String, Long] = MMap[String, Long]()
    
    def update(newId: String) {
      this.synchronized { syncMap(newId) = System.currentTimeMillis() } }
    
    def size(): Int = {
      this.synchronized { return syncMap.size } }
    
    def prune(timeout: Long) {
      this.synchronized { 
        var newMap: MMap[String, Long] = MMap[String, Long]()
        var curTime: Long = System.currentTimeMillis()
        syncMap.foreach{ keyVal => 
          if(curTime - keyVal._2 <= timeout) { 
            newMap(keyVal._1) = keyVal._2
          } 
        }
        syncMap = newMap
      } 
    }

    def toSet(): Set[String] = {
      this.synchronized {
        var set: Set[String] = Set()
        syncMap.foreach { keyVal => set = set + keyVal._1}
        return set
      }
    }

  }

  var controllerIdMap = new SyncIdMap()
  var controllerMapResetInterval: Long = 10*1000 // 10 seconds to ms

  /**
   * Factory used by the ContainerProxy to physically create a new container.
   *
   * Create and initialize the container factory before kicking off any other
   * task or actor because further operation does not make sense if something
   * goes wrong here. Initialization will throw an exception upon failure.
   */
  private val containerFactory =
    SpiLoader
      .get[ContainerFactoryProvider]
      .instance(
        actorSystem,
        logging,
        config,
        instance,
        Map(
          "--cap-drop" -> Set("NET_RAW", "NET_ADMIN"),
          "--ulimit" -> Set("nofile=1024:1024"),
          "--pids-limit" -> Set("1024")) ++ logsProvider.containerParameters)
  containerFactory.init()

  CoordinatedShutdown(actorSystem)
    .addTask(CoordinatedShutdown.PhaseBeforeActorSystemTerminate, "cleanup runtime containers") { () =>
      containerFactory.cleanup()
      Future.successful(Done)
    }

  /** Initialize needed databases */
  private val entityStore = WhiskEntityStore.datastore()
  private val activationStore =
    SpiLoader.get[ActivationStoreProvider].instance(actorSystem, materializer, logging)

  private val authStore = WhiskAuthStore.datastore()

  private val namespaceBlacklist = new NamespaceBlacklist(authStore)

  Scheduler.scheduleWaitAtMost(loadConfigOrThrow[NamespaceBlacklistConfig](ConfigKeys.blacklist).pollInterval) { () =>
    logging.debug(this, "running background job to update blacklist")
    namespaceBlacklist.refreshBlacklist()(ec, TransactionId.invoker).andThen {
      case Success(set) => logging.info(this, s"updated blacklist to ${set.size} entries")
      case Failure(t)   => logging.error(this, s"error on updating the blacklist: ${t.getMessage}")
    }
  }

  /** Initialize message consumers */
  private val topic = s"invoker${instance.toInt}"
  private val maximumContainers = (poolConfig.userMemory / MemoryLimit.MIN_MEMORY).toInt
  private val msgProvider = SpiLoader.get[MessagingProvider]

  //number of peeked messages - increasing the concurrentPeekFactor improves concurrent usage, but adds risk for message loss in case of crash
  private val maxPeek =
    math.max(maximumContainers, (maximumContainers * limitsConfig.max * poolConfig.concurrentPeekFactor).toInt)

  private val consumer =
    msgProvider.getConsumer(config, topic, topic, maxPeek, maxPollInterval = TimeLimit.MAX_DURATION + 1.minute)

  private val activationFeed = actorSystem.actorOf(Props {
    new MessageFeed("activation", logging, consumer, maxPeek, 1.second, processActivationMessage)
  })

  /** Sends an active-ack. */
  // yanqi, add cpu util & execution time
  private val ack: InvokerReactive.ActiveAck = (tid: TransactionId,
                                                activationResult: WhiskActivation,
                                                blockingInvoke: Boolean,
                                                controllerInstance: ControllerInstanceId,
                                                userId: UUID,
                                                isSlotFree: Boolean,
                                                cpuUtil: Double,
                                                exeTime: Long,  // unit is ms
                                                totalTime: Long) => {
    implicit val transid: TransactionId = tid

    def send(res: Either[ActivationId, WhiskActivation], recovery: Boolean = false) = {
      val msg = if (isSlotFree) {
        val aid = res.fold(identity, _.activationId)
        val isWhiskSystemError = res.fold(_ => false, _.response.isWhiskError)
        // yanqi, add cpuUtil & exeTime & totalTime to CompletionMessage
        CompletionMessage(transid, aid, isWhiskSystemError, instance, cpuUtil, exeTime, totalTime)
      } else {
        ResultMessage(transid, res)
      }

      producer.send(topic = "completed" + controllerInstance.asString, msg).andThen {
        case Success(_) =>
          logging.info(
            this,
            s"posted ${if (recovery) "recovery" else "completion"} of activation ${activationResult.activationId}")
      }
    }

    // UserMetrics are sent, when the slot is free again. This ensures, that all metrics are sent.
    if (UserEvents.enabled && isSlotFree) {
      EventMessage.from(activationResult, s"invoker${instance.instance}", userId) match {
        case Success(msg) => UserEvents.send(producer, msg)
        case Failure(t)   => logging.error(this, s"activation event was not sent: $t")
      }
    }

    send(Right(if (blockingInvoke) activationResult else activationResult.withoutLogsOrResult)).recoverWith {
      case t if t.getCause.isInstanceOf[RecordTooLargeException] =>
        send(Left(activationResult.activationId), recovery = true)
    }
  }

  /** Stores an activation in the database. */
  private val store = (tid: TransactionId, activation: WhiskActivation, context: UserContext) => {
    implicit val transid: TransactionId = tid
    activationStore.storeAfterCheck(activation, context)(tid, notifier = None)
  }

  /** Creates a ContainerProxy Actor when being called. */
  private val childFactory = (f: ActorRefFactory) =>
    f.actorOf(
      ContainerProxy
        .props(containerFactory.createContainer, ack, store, logsProvider.collectLogs, instance, poolConfig))

  val prewarmingConfigs: List[PrewarmingConfig] = {
    ExecManifest.runtimesManifest.stemcells.flatMap {
      case (mf, cells) =>
        cells.map { cell =>
          // yanqi, assume 1 core for all prewarmed containers
          PrewarmingConfig(cell.count, new CodeExecAsString(mf, "", None), 1, cell.memory)
        }
    }.toList
  }

  private val pool =
    actorSystem.actorOf(ContainerPool.props(childFactory, poolConfig, activationFeed, prewarmingConfigs))

  /** Is called when an ActivationMessage is read from Kafka */
  def processActivationMessage(bytes: Array[Byte]): Future[Unit] = {
    Future(ActivationMessage.parse(new String(bytes, StandardCharsets.UTF_8)))
      .flatMap(Future.fromTry)
      .flatMap { msg =>
        // The message has been parsed correctly, thus the following code needs to *always* produce at least an
        // active-ack.

        implicit val transid: TransactionId = msg.transid

        //set trace context to continue tracing
        WhiskTracerProvider.tracer.setTraceContext(transid, msg.traceContext)

        // update controllerIdMap. yanqi
        controllerIdMap.update(msg.rootControllerIndex.asString)

        if (!namespaceBlacklist.isBlacklisted(msg.user)) {
          val start = transid.started(this, LoggingMarkers.INVOKER_ACTIVATION, logLevel = InfoLevel)
          val namespace = msg.action.path
          val name = msg.action.name
          val actionid = FullyQualifiedEntityName(namespace, name).toDocId.asDocInfo(msg.revision)
          val subject = msg.user.subject

          logging.debug(this, s"${actionid.id} $subject ${msg.activationId}")

          // caching is enabled since actions have revision id and an updated
          // action will not hit in the cache due to change in the revision id;
          // if the doc revision is missing, then bypass cache
          if (actionid.rev == DocRevision.empty) logging.warn(this, s"revision was not provided for ${actionid.id}")

          WhiskAction
            .get(entityStore, actionid.id, actionid.rev, fromCache = actionid.rev != DocRevision.empty)
            .flatMap { action =>
              action.toExecutableWhiskAction match {
                case Some(executable) =>
                  pool ! Run(executable, msg)
                  Future.successful(())
                case None =>
                  logging.error(this, s"non-executable action reached the invoker ${action.fullyQualifiedName(false)}")
                  Future.failed(new IllegalStateException("non-executable action reached the invoker"))
              }
            }
            .recoverWith {
              case t =>
                // If the action cannot be found, the user has concurrently deleted it,
                // making this an application error. All other errors are considered system
                // errors and should cause the invoker to be considered unhealthy.
                val response = t match {
                  case _: NoDocumentException =>
                    ActivationResponse.applicationError(Messages.actionRemovedWhileInvoking)
                  case _: DocumentTypeMismatchException | _: DocumentUnreadable =>
                    ActivationResponse.whiskError(Messages.actionMismatchWhileInvoking)
                  case _ =>
                    ActivationResponse.whiskError(Messages.actionFetchErrorWhileInvoking)
                }

                val context = UserContext(msg.user)
                val activation = generateFallbackActivation(msg, response)
                activationFeed ! MessageFeed.Processed
                // yanqi, add 0.0 as default cpu util & 0 as default execution time & total time
                ack(msg.transid, activation, msg.blocking, msg.rootControllerIndex, msg.user.namespace.uuid, true, 0.0, 0, 0)   
                store(msg.transid, activation, context)
                Future.successful(())
            }
        } else {
          // Iff the current namespace is blacklisted, an active-ack is only produced to keep the loadbalancer protocol
          // Due to the protective nature of the blacklist, a database entry is not written.
          activationFeed ! MessageFeed.Processed
          val activation =
            generateFallbackActivation(msg, ActivationResponse.applicationError(Messages.namespacesBlacklisted))
            // yanqi, add 0.0 as default cpu util & 0 as default execution time & total time
          ack(msg.transid, activation, false, msg.rootControllerIndex, msg.user.namespace.uuid, true, 0.0, 0, 0)    
          logging.warn(this, s"namespace ${msg.user.namespace.name} was blocked in invoker.")
          Future.successful(())
        }
      }
      .recoverWith {
        case t =>
          // Iff everything above failed, we have a terminal error at hand. Either the message failed
          // to deserialize, or something threw an error where it is not expected to throw.
          activationFeed ! MessageFeed.Processed
          logging.error(this, s"terminal failure while processing message: $t")
          Future.successful(())
      }
  }

  /** Generates an activation with zero runtime. Usually used for error cases */
  private def generateFallbackActivation(msg: ActivationMessage, response: ActivationResponse): WhiskActivation = {
    val now = Instant.now
    val causedBy = if (msg.causedBySequence) {
      Some(Parameters(WhiskActivation.causedByAnnotation, JsString(Exec.SEQUENCE)))
    } else None

    WhiskActivation(
      activationId = msg.activationId,
      namespace = msg.user.namespace.name.toPath,
      subject = msg.user.subject,
      cause = msg.cause,
      name = msg.action.name,
      version = msg.action.version.getOrElse(SemVer()),
      start = now,
      end = now,
      duration = Some(0),
      response = response,
      annotations = {
        Parameters(WhiskActivation.pathAnnotation, JsString(msg.action.asString)) ++ causedBy
      })
  }

  // yanqi, produce health ping message, here we can piggyback the resource information
  // plus the gossip information telling each controller how many controllers there are in the system
  private val healthProducer = msgProvider.getProducer(config)
  Scheduler.scheduleWaitAtMost(1.seconds)(() => {
    var cpu: Int = 1
    var memory: Int = 2048

    controllerIdMap.prune(controllerMapResetInterval)
    var controller_set: Set[String] = controllerIdMap.toSet()

    // check total available resources
    if(Files.exists(Paths.get(resourcePath))) {
      val buffer_kvp = Source.fromFile(resourcePath)
      val lines_kvp = buffer_kvp.getLines.toArray
      
      if(lines_kvp.size == 2) {
        cpu = lines_kvp(0).toInt
        memory = lines_kvp(1).toInt
      }
      buffer_kvp.close
    }

    var rscFileExists: Boolean = true
    // check actual cpu usage
    if(Files.exists(Paths.get(cgroupCpuPath))) {
      val buffer_cgroup_cpu = Source.fromFile(cgroupCpuPath)
      val lines_cgroup = buffer_cgroup_cpu.getLines.toArray
      var cpu_time: Long = 0
      
      if(lines_cgroup.size == 1) {
        cpu_time = lines_cgroup(0).toLong
      }
      if(cgroupCheckTime == 0) {
        cgroupCheckTime = System.nanoTime
        cgroupCpuTime = cpu_time
      } else {
        var curns: Long = System.nanoTime
        // update
        cgroupCpuUsage = ((cpu_time - cgroupCpuTime).toDouble / (curns - cgroupCheckTime))
        cgroupCpuUsage = (cgroupCpuUsage * 1000).toInt/1000.0

        cgroupCheckTime = curns
        cgroupCpuTime = cpu_time
      }
      buffer_cgroup_cpu.close
    } else {
      logging.warn(this, s"${cgroupCpuPath} does not exist")
      rscFileExists = false
    }

    // check actual memory usage
    if(Files.exists(Paths.get(cgroupMemPath))) {
      val buffer_cgroup_mem = Source.fromFile(cgroupMemPath)
      val lines_cgroup = buffer_cgroup_mem.getLines.toArray
      var mem_usage: Int = 0  

      var total_cache: Int = 0
      var total_rss: Int = 0

      for(line <- lines_cgroup) {
        if(line.contains("total_cache")) {
          total_cache = line.split(" ")(1).toInt/(1024*1024)
        } else if(line.contains("total_rss")) {
          total_rss = line.split(" ")(1).toInt/(1024*1024)
        }
      }
      buffer_cgroup_mem.close
      cgroupMemUsage = total_cache + total_rss
    } else {
      logging.warn(this, s"${cgroupMemPath} does not exist")
      rscFileExists = false
    }
    var mean_cpu_usage: Double = 0.0
    var mean_mem_usage: Int = 0
    var max_mem_usage: Int = 0
    if(rscFileExists) {
      cgroupWindow(cgroupWindowPtr) = (cgroupCpuUsage, cgroupMemUsage)
      proceed_cgroup_window_ptr()
      val (c, m) = get_mean_rsc_usage()
      mean_cpu_usage = c
      mean_mem_usage = m
      val (c_m, m_m) = get_max_rsc_usage()
      max_mem_usage = m_m
      logging.info(this, s"healthPing cgroupCpuUsage, cgroupMemUsage = ${cgroupCpuUsage}, ${cgroupMemUsage}")
      logging.info(this, s"healthPing mean_cgroupCpuUsage, mean_cgroupMemUsage, max_cgroupMemUsage = ${mean_cpu_usage}, ${mean_mem_usage}, ${max_mem_usage}")
    }
    
    healthProducer.send("health", PingMessage(
        instance, cpu, memory, 
        mean_cpu_usage, max_mem_usage,
        controller_set)).andThen {
      case Failure(t) => logging.error(this, s"failed to ping the controller: $t")
      // case Success(_) => logging.info(this, s"heartbeat -- cpu: ${cpu}, memory ${memory}, linenum ${lines.size}")
    }
  })
  // private val healthProducer = msgProvider.getProducer(config)
  // Scheduler.scheduleWaitAtMost(1.seconds)(() => {
  //   healthProducer.send("health", PingMessage(instance)).andThen {
  //     case Failure(t) => logging.error(this, s"failed to ping the controller: $t")
  //   }
  // })
}
