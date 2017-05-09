package io.exemplary.akka.discovery

import java.util.Date

import com.amazonaws.services.autoscaling.AmazonAutoScalingClient
import com.amazonaws.services.autoscaling.model.{Instance => ScalingInstance, _}
import com.amazonaws.services.ec2.AmazonEC2Client
import com.amazonaws.services.ec2.model.{Instance => Ec2Instance, _}
import io.exemplary.akka.discovery.ClusterDiscoveryTest.{Given, When, Then}
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.{FeatureSpec, Matchers}

import scala.collection.JavaConverters._

class ClusterDiscoveryTest extends FeatureSpec with Matchers {

  val LaunchTime = new Date()
  val LaunchTimeBefore = new Date(LaunchTime.getTime - 1000)
  val LaunchTimeAfter = new Date(LaunchTime.getTime + 1000)
  val Running = new InstanceState().withName(InstanceStateName.Running)
  val NotRunning = new InstanceState().withName(InstanceStateName.Stopped)
  val ScalingGroupName = "group"
  val Ec2InstanceId_A = "instanceId_A"
  val Ec2InstanceId_B = "instanceId_B"
  val Ec2InstanceId_C = "instanceId_C"
  val PrivateIp_A = "ip_A"
  val PrivateIp_B = "ip_B"
  val PrivateIp_C = "ip_C"

  feature("discover") {

    scenario("only one running instance") {
      val instance = given.an_instance(Ec2InstanceId_A, LaunchTime, Running, PrivateIp_A)
      val scalingGroup = given.a_scaling_group(ScalingGroupName).with_instances(instance)
      val seedUrls = when.the(scalingGroup).discovers_seed_urls_for_the(instance)
      then.the(seedUrls)
        .should_be(1)
        .and_seed_url(0).should_be(instance)
    }

    scenario("two running instances for the old instance, one for the other instance") {
      val olderInstance = given.an_instance(Ec2InstanceId_A, LaunchTime, Running, PrivateIp_A)
      val youngerInstance = given.an_instance(Ec2InstanceId_B, LaunchTimeAfter, Running, PrivateIp_B)
      val scalingGroup = given.a_scaling_group(ScalingGroupName).with_instances(olderInstance, youngerInstance)
      val olderSeedUrls = when.the(scalingGroup).discovers_seed_urls_for_the(olderInstance)
      then.the(olderSeedUrls)
        .should_be(2)
        .and_seed_url(0).should_be(olderInstance)
        .and_seed_url(1).should_be(youngerInstance)
      val youngerSeedUrls = when.the(scalingGroup).discovers_seed_urls_for_the(youngerInstance)
      then.the(youngerSeedUrls)
        .should_be(1)
        .and_seed_url(0).should_be(olderInstance)
    }

    scenario("the oldest instance should see all instances ordered by lunchTime") {
      val olderInstance = given.an_instance(Ec2InstanceId_A, LaunchTimeBefore, Running, PrivateIp_A)
      val middleInstance = given.an_instance(Ec2InstanceId_B, LaunchTime, Running, PrivateIp_B)
      val youngerInstance = given.an_instance(Ec2InstanceId_C, LaunchTimeAfter, Running, PrivateIp_C)
      val scalingGroup = given.a_scaling_group(ScalingGroupName)
        .with_instances(olderInstance, middleInstance, youngerInstance)
      val seedUrls = when.the(scalingGroup).discovers_seed_urls_for_the(olderInstance)
      then.the(seedUrls)
        .should_be(3)
        .and_seed_url(0).should_be(olderInstance)
        .and_seed_url(1).should_be(middleInstance)
        .and_seed_url(2).should_be(youngerInstance)
    }

    scenario("the not oldest instance should not see itself, only the others") {
      val olderInstance = given.an_instance(Ec2InstanceId_A, LaunchTimeBefore, Running, PrivateIp_A)
      val middleInstance = given.an_instance(Ec2InstanceId_B, LaunchTime, Running, PrivateIp_B)
      val youngerInstance = given.an_instance(Ec2InstanceId_C, LaunchTimeAfter, Running, PrivateIp_C)
      val scalingGroup = given.a_scaling_group(ScalingGroupName)
        .with_instances(olderInstance, middleInstance, youngerInstance)
      val seedUrls = when.the(scalingGroup).discovers_seed_urls_for_the(middleInstance)
      then.the(seedUrls)
        .should_be(2)
        .and_seed_url(0).should_be(olderInstance)
        .and_seed_url(1).should_be(youngerInstance)
    }

    scenario("should not see not running instances") {
      val olderInstance = given.an_instance(Ec2InstanceId_A, LaunchTimeBefore, Running, PrivateIp_A)
      val notRunngingInstance = given.an_instance(Ec2InstanceId_B, LaunchTime, NotRunning, PrivateIp_B)
      val youngerInstance = given.an_instance(Ec2InstanceId_C, LaunchTimeAfter, Running, PrivateIp_C)
      val scalingGroup = given.a_scaling_group(ScalingGroupName)
        .with_instances(olderInstance, notRunngingInstance, youngerInstance)
      val seedUrls = when.the(scalingGroup).discovers_seed_urls_for_the(olderInstance)
      then.the(seedUrls)
        .should_be(2)
        .and_seed_url(0).should_be(olderInstance)
        .and_seed_url(1).should_be(youngerInstance)
    }

  }

  val given = new Given
  val when = new When
  val then = new Then

}

object ClusterDiscoveryTest {

  val Port = 2551

  class Given {

    def an_instance(instanceId: String, launchTime: Date, state: InstanceState, privateIp: String): Ec2Instance = {
      new Ec2Instance()
        .withInstanceId(instanceId)
        .withLaunchTime(launchTime)
        .withState(state)
        .withPrivateIpAddress(privateIp)
    }

    def a_scaling_group(name: String) = GivenScalingGroup(name)

  }

  case class GivenScalingGroup(name: String, instances: List[Ec2Instance] = Nil) {

    private val instanceIds = instances.map(_.getInstanceId)

    private def scalingClient = withScalingClient(
      toGroupName = { case instanceId if instanceIds.contains(instanceId) => name },
      toInstanceIds = { case groupName if groupName == name => instanceIds  }
    )

    private def ec2Client = withEc2Client(
      toInstance = { case instanceId if instanceIds.contains(instanceId) => instances.find(_.getInstanceId == instanceId).get }
    )

    def with_instances(instances: Ec2Instance*) = copy(instances = instances.toList)

    def discovers_seed_urls_for_the(instance: Ec2Instance): List[String] = {
      val discovery = new MockClusterDiscovery(instance.getInstanceId, ec2Client, scalingClient)
      discovery.discoverSeedUrls(Port)
    }

  }

  class When {

    def the(scalingGroup: GivenScalingGroup) = scalingGroup

  }

  class MockClusterDiscovery(instanceId: String, ec2Client: AmazonEC2Client, scalingClient: AmazonAutoScalingClient)
    extends ClusterDiscovery(ec2Client, scalingClient) {
    override private[discovery] lazy val ec2InstanceId: Ec2InstanceId = instanceId
  }

  def withEc2Client(toInstance: PartialFunction[String, Ec2Instance]): AmazonEC2Client = {
    val client = mock(classOf[AmazonEC2Client])
    when(client.describeInstances(any[DescribeInstancesRequest]))
      .then(new Answer[DescribeInstancesResult] {
        def answer(invocation: InvocationOnMock): DescribeInstancesResult = {
          val request = invocation.getArguments()(0).asInstanceOf[DescribeInstancesRequest]
          val reservation = new Reservation
          request.getInstanceIds.asScala.collect(toInstance).foreach(instance => reservation.withInstances(instance))
          new DescribeInstancesResult().withReservations(reservation)
        }
      })
    client
  }

  def withScalingClient(toGroupName: PartialFunction[String, String], toInstanceIds: PartialFunction[String, List[String]]): AmazonAutoScalingClient = {
    val client = mock(classOf[AmazonAutoScalingClient])
    when(client.describeAutoScalingInstances(any[DescribeAutoScalingInstancesRequest]))
      .then(new Answer[DescribeAutoScalingInstancesResult] {
        def answer(invocation: InvocationOnMock): DescribeAutoScalingInstancesResult = {
          val request = invocation.getArguments()(0).asInstanceOf[DescribeAutoScalingInstancesRequest]
          val result = new DescribeAutoScalingInstancesResult()
          request.getInstanceIds.asScala.collect(toGroupName).foreach(groupName => {
            val detail = new AutoScalingInstanceDetails().withAutoScalingGroupName(groupName)
            result.withAutoScalingInstances(detail)
          })
          result
        }
      })
    when(client.describeAutoScalingGroups(any[DescribeAutoScalingGroupsRequest]))
      .then(new Answer[DescribeAutoScalingGroupsResult] {
        def answer(invocation: InvocationOnMock): DescribeAutoScalingGroupsResult = {
          val request = invocation.getArguments()(0).asInstanceOf[DescribeAutoScalingGroupsRequest]
          val result = new DescribeAutoScalingGroupsResult
          request.getAutoScalingGroupNames.asScala.collect(toInstanceIds).foreach(instanceIds => {
            val scalingGroup = new AutoScalingGroup
            instanceIds.foreach(instanceId => {
              val instance = new ScalingInstance().withInstanceId(instanceId)
              scalingGroup.withInstances(instance)
            })
            result.withAutoScalingGroups(scalingGroup)
          })
          result
        }
      })
    client
  }

  class Then {

    def the(seedUrls: List[String]) = new ThenSeeds(seedUrls)

  }

  class ThenSeeds(urls: List[String]) extends Matchers {

    def should_be(number: Int): ThenSeeds = {
      urls should have size number
      this
    }

    def and_seed_url(index: Int) = new ThenSeed(urls(index))


    class ThenSeed(url: String) {

      def should_be(instance: Ec2Instance): ThenSeed = {
        url should be (s"${instance.getPrivateIpAddress}:$Port")
        this
      }

      def and_seed_url(index: Int) = new ThenSeed(urls(index))

    }

  }

}
