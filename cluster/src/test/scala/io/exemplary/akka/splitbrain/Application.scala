package io.exemplary.akka.splitbrain

import akka.actor.ActorContext
import akka.actor.{ActorSystem, Props}
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import io.exemplary.akka.splitbrain.Application.{Backdoor, SplitBrainResolverWithBackdoor}

import scala.concurrent.{Await, Promise}
import scala.concurrent.duration._

class Application(port: Int, seedPorts: List[Int] = Nil) {

  private val clusterName = "ClusterName"
  private val seeds = seedPorts.map(port => s"akka.tcp://$clusterName@localhost:$port")
  private val config = ConfigFactory.parseString(configuration(port, seeds))
  private val system = ActorSystem(clusterName, config)
  val backdoor = new Backdoor[Set[ClusterMember]]
  private val splitBrainResolver = system.actorOf(Props(classOf[SplitBrainResolverWithBackdoor], backdoor), s"split-brain-resolver-$port")

  def getClusterMembers: Set[ClusterMember] = {
    implicit val timeout = Timeout(2.seconds)
    val futureResult = (splitBrainResolver ? "get").mapTo[Set[ClusterMember]]
    Await.result(futureResult, 2.seconds)
  }

  def shutdown = Await.ready(system.terminate(), 2.seconds)

  private def configuration(port: Int, seeds: List[String]): String = {
    val seed_nodes = seeds.map(seed => s"""\"$seed\"""").mkString(",")
    s"""
       |akka {
       |  actor {
       |    provider = "cluster"
       |  }
       |  remote {
       |    log-remote-lifecycle-events = off
       |    netty.tcp {
       |      hostname = "localhost"
       |      port = $port
       |    }
       |  }
       |  cluster {
       |    seed-nodes = [$seed_nodes]
       |  }
       |}
    """.stripMargin
  }
}

object Application {

  class SplitBrainResolverWithBackdoor(backdoor: Backdoor[Set[ClusterMember]]) extends SplitBrainResolver {
    override protected[splitbrain] val cluster: ClusterMembers = new ClusterMembersWithBackdoor(backdoor, context)
  }

  class ClusterMembersWithBackdoor(backdoor: Backdoor[Set[ClusterMember]], actorContext: ActorContext)
    extends ClusterMembers(actorContext) {
    override protected[splitbrain] def updateMembers(members: Set[ClusterMember]): Unit = {
      backdoor.update(members)
      super.updateMembers(members)
    }
  }

  class Backdoor[T] {

    private var promise = Promise[T]()

    def update(item: T): T = {
      promise.success(item)
      promise = Promise[T]()
      item
    }

    def getUpdatePromise = promise

  }

}
