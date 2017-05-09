package io.exemplary.akka.splitbrain

import akka.actor.{Actor, ActorLogging, Address, Cancellable}
import akka.cluster.ClusterEvent._
import akka.cluster.Member
import com.amazonaws.services.autoscaling.AmazonAutoScalingAsyncClient
import com.amazonaws.services.autoscaling.model.{DescribeAutoScalingGroupsRequest, DescribeAutoScalingInstancesRequest}
import com.amazonaws.services.ec2.AmazonEC2AsyncClient
import com.amazonaws.services.ec2.model.DescribeInstancesRequest
import com.amazonaws.util.EC2MetadataUtils
import io.exemplary.akka.splitbrain.SplitBrainResolver._

import scala.collection.JavaConverters._
import scala.concurrent.duration._

class SplitBrainResolver extends Actor with ShutDownMember with ActorLogging {

  protected[splitbrain] val cluster = new ClusterMembers(context)

  implicit val executionContext = context.dispatcher

  override def preStart() {
    shutDownMemberOnTermination()
    cluster.subscribeClusterMemberUpdates()
  }

  def receive = cluster.clusterMemberUpdates orElse reachableUpdates orElse splitBrainUpdate

  /**
    * Each new member must be populated with the instance id,
    * value that is necessary to check if the instance is healthy
    * or not. I should check if I can send this information as part
    * of the cluster. It would be amazing.
    */
  def reachableUpdates: Receive = {
    case UnreachableMember(member) =>
      unreachableMember(member)
    case ReachableMember(member) =>
      reachableMemeber(member)
  }

  def splitBrainUpdate: Receive = {
    case StopMember(member) =>
      if (cluster.isLeader) {
        cluster.stopMember(member)
      }
    case StopPartition() =>
      stopTheNetworkPartiotion()
  }

  def reachableMemeber(member: Member) = {
    cluster.setAsReachable(member)
    if (cluster.noUnreachableMembers()) {
      cancelStableTimer()
      cancelUnreachableTimer()
    }
  }

  /**
    * 1. Set the member unreachable
    * 3. If the instance is not Healthy (according with AWS) a message is sent to stop it.
    * 4. We are in presence of a network partition (possible network partition).
    * 4.1 Stop the minority without leader
    */
  def unreachableMember(member: Member) = {
    cluster.setAsUnreachable(member)
    if (!isHealthy(member.withInstanceId)) {
      self ! StopMember(member)
    } else {
      cancelStableTimer()
      startStableTimer()
      startUnreachableTimer()
    }
  }

  val stableDelay = 6.seconds
  var stableTimer: Option[Cancellable] = None

  def cancelStableTimer() = {
    stableTimer.foreach(_.cancel())
    stableTimer = None
  }

  def startStableTimer() = stableTimer = Some(context.system.scheduler.scheduleOnce(stableDelay, self, StopPartition()))

  val unreachableDelay = 10.seconds
  var unreachableTimer: Option[Cancellable] = None

  def cancelUnreachableTimer() = {
    unreachableTimer.foreach(_.cancel())
    unreachableTimer = None
  }

  def startUnreachableTimer() = if (unreachableTimer.isEmpty) {
    unreachableTimer = Some(context.system.scheduler.scheduleOnce(unreachableDelay, self, StopPartition()))
  }

  /**
    * Stop the one network partition, the one contains
    * less member if there is no leader. or if there is
    * the leader stop if very small ... ?
    */
  def stopTheNetworkPartiotion() {
    if (isLeaderUnreachable) {
      if (unreachablePercentage > 0.6) stopReachable()
      else stopUnreachable()
    } else {
      if (unreachablePercentage <= 0.6) stopReachable()
      else stopUnreachable()
    }
  }

  def isLeaderUnreachable = cluster.getMembers.filter(_.unreachable).exists(_.leader)

  def unreachablePercentage: Double = {
    val clusterSize = cluster.getMembers.size
    val unreachableMembersSize = cluster.getMembers.count(_.unreachable)
    unreachableMembersSize.toDouble / clusterSize.toDouble
  }

  def stopReachable() = {
    cluster.getMembers.filter(!_.unreachable).map(_.member).foreach(cluster.stopMember)
  }

  def stopUnreachable() = {
    cluster.getMembers.filter(_.unreachable).map(_.member).foreach(cluster.stopMember)
  }

}

object SplitBrainResolver {

  case class AwsMember(member: Member, instanceId: Option[AwsInstanceId] = None, address: Address)

  sealed trait SplitBrainEvent

  case class StopMember(member: Member) extends SplitBrainEvent

  case class StopPartition() extends SplitBrainEvent

  val autoScalingClient = new AmazonAutoScalingAsyncClient
  val ec2Client = new AmazonEC2AsyncClient

  lazy val instanceId = EC2MetadataUtils.getInstanceId

  lazy val scalingGroupName = {
    val result = autoScalingClient.describeAutoScalingInstances(new DescribeAutoScalingInstancesRequest().withInstanceIds(instanceId))
    result.getAutoScalingInstances.asScala.map( instance => instance.getAutoScalingGroupName ).head
  }

  implicit def toAwsMember(member: Member): AwsMemberCreator = new AwsMemberCreator(member, scalingGroupName)

  class AwsMemberCreator(member: Member, scalingGroupName: String) {

    val awsMember = AwsMember(member, address = member.address)

    def withInstanceId: AwsMember = {
      val instanceIds = getScalingGroupInstanceIds(scalingGroupName)
      val instances = getInstances(instanceIds)
      val optionInstanceId = instances.find( instance => instance.getPrivateIpAddress == member.address.host.get )
        .map ( instance => instance.getInstanceId )
      awsMember.copy( instanceId = optionInstanceId )
    }

  }

  private def getScalingGroupInstanceIds(groupName: String) = {
    val r = autoScalingClient.describeAutoScalingGroups(new DescribeAutoScalingGroupsRequest().withAutoScalingGroupNames(groupName))
    r.getAutoScalingGroups.asScala.headOption.map( group => group.getInstances.asScala.toList.map(_.getInstanceId) ).get
  }

  private def getInstances(instanceIds: List[String]) = {
    val result = ec2Client.describeInstances(new DescribeInstancesRequest().withInstanceIds(instanceIds.asJavaCollection))
    result.getReservations.asScala.toList.flatMap( reservation => reservation.getInstances.asScala.toList )
  }

  def isHealthy(member: AwsMember): Boolean = {
    member.instanceId.map( isHealth ).get
  }

  val Healthy = "Healthy"

  private def isHealth(instanceId: AwsInstanceId) = {
    autoScalingClient.describeAutoScalingInstances(new DescribeAutoScalingInstancesRequest().withInstanceIds(instanceId))
      .getAutoScalingInstances.asScala.map( instance => instance.getHealthStatus == Healthy ).head
  }

}
