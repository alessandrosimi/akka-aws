package io.exemplary.akka.splitbrain

import akka.actor.{Actor, ActorSystem}
import akka.cluster.Cluster

import scala.concurrent.Await
import scala.concurrent.duration._

trait ShutDownMember {
  actor: Actor =>

  protected val JvmShutDownTimeout = 10.seconds
  protected val ActorSystemShutDownTimeout = 5.seconds

  def shutDownMemberOnTermination() = Cluster(context.system)
    .registerOnMemberRemoved {
      // exit JVM when ActorSystem has been terminated
      context.system.registerOnTermination(jvmShutDown())
      // in case ActorSystem shutdown takes longer than 10 seconds,
      // exit the JVM forcefully anyway
      context.system.scheduler.scheduleOnce(JvmShutDownTimeout)(jvmShutDown())(context.system.dispatcher)
      // shut down ActorSystem
      Await.ready(actorSystemShutDown(), ActorSystemShutDownTimeout)
    }

  private def jvmShutDown() = System.exit(-1)

  private def actorSystemShutDown() = context.system.terminate()

}
