package io.exemplary.akka.discovery

import com.amazonaws.services.autoscaling.AmazonAutoScaling
import com.amazonaws.services.autoscaling.model.{DescribeAutoScalingGroupsRequest, DescribeAutoScalingInstancesRequest}
import com.amazonaws.services.ec2.AmazonEC2
import com.amazonaws.services.ec2.model.{DescribeInstancesRequest, Instance, InstanceStateName}
import com.amazonaws.util.EC2MetadataUtils

import scala.collection.JavaConverters._

/**
  * Utility class to discover instances in the same scaling group.
  * @param ec2Client aws ec2 client
  * @param scalingClient aws scaling client
  */
class ClusterDiscovery(ec2Client: AmazonEC2, scalingClient: AmazonAutoScaling) {

  type Ec2InstanceId = String
  type ScalingGroupName = String
  type Ec2Instance = Instance

  /**
    * All the instances have start the cluster on the same port.
    * @param port the instance port.
    * @return the list of urls of the other instances inside the scaling group.
    */
  def discoverSeedUrls(port: Int): List[String] = {
    val ec2Instances = getEc2InstancesFromScalingGroup
    val runningEc2Instances = ec2Instances.filter(isRunning)
    val ec2InstanceOrderedByLaunchTime = runningEc2Instances.sortBy(_.getLaunchTime.getTime)
    val adjust = ec2InstanceOrderedByLaunchTime match {
      case head :: tail if head.getInstanceId == ec2InstanceId => head :: tail
      case rest => rest.filter(_.getInstanceId != ec2InstanceId)
    }
    adjust.map(instance => s"${instance.getPrivateIpAddress}:$port")
  }

  private[discovery] lazy val ec2InstanceId: Ec2InstanceId = EC2MetadataUtils.getInstanceId

  private lazy val scalingGroupName = {
    val request = new DescribeAutoScalingInstancesRequest().withInstanceIds(ec2InstanceId)
    scalingClient.describeAutoScalingInstances(request)
      .getAutoScalingInstances.asScala.headOption
      .map(_.getAutoScalingGroupName)
  }

  private def getEc2InstancesFromScalingGroup: List[Ec2Instance] = {
    val scalingGroup = scalingGroupName.flatMap { name =>
      val request = new DescribeAutoScalingGroupsRequest().withAutoScalingGroupNames(name)
      scalingClient.describeAutoScalingGroups(request).getAutoScalingGroups.asScala.headOption
    }
    scalingGroup
      .map(scalingGroup => scalingGroup.getInstances.asScala.toList)
      .getOrElse(Nil)
      .map(instance => instance.getInstanceId)
      .map(toEc2Instance)
      .collect { case Some(ec2Instance) => ec2Instance }
  }

  private def toEc2Instance(instanceId: Ec2InstanceId): Option[Ec2Instance] = {
    val request = new DescribeInstancesRequest().withInstanceIds(instanceId)
    ec2Client.describeInstances(request)
      .getReservations.asScala.headOption
      .flatMap(
        reservation => reservation.getInstances.asScala.headOption
      )
  }

  private def isRunning(ec2Instance: Ec2Instance): Boolean = {
    InstanceStateName.fromValue(ec2Instance.getState.getName) == InstanceStateName.Running
  }

}
