package io.exemplary.akka.splitbrain

import akka.actor.ActorContext
import akka.actor.Actor.Receive
import akka.actor.Address
import akka.cluster.ClusterEvent._
import akka.cluster.{Cluster, Member}
import akka.event.Logging

class ClusterMembers(context: ActorContext) {

  val log = Logging.getLogger(context.system, this)

  def subscribeClusterMemberUpdates() = Cluster(context.system).subscribe(context.self, classOf[ClusterDomainEvent])

  var leaderAddress: Option[Address] = None
  private var members = Set[ClusterMember]()

  def getMembers = members

  def clusterMemberUpdates: Receive = init orElse membersUpdates orElse leaderUpdates orElse getClusterMembers

  private def init: Receive = {
    case state: CurrentClusterState =>
      log.info(s"Received current cluster state [$state]")
//      members = state.getMembers.asScala.toSet.map( member => ClusterMember(member) )
      setTheLeader(state.leader)
      leaderAddress = state.leader
  }

  protected[splitbrain] def updateMembers(members: Set[ClusterMember]): Unit = {
    this.members = members
  }

  private def setTheLeader(leaderAddress: Option[Address]) = {
    updateMembers(members.map( member => if (leaderAddress.contains(member.address)) member.copy(leader = true) else member ))
  }

  private def getClusterMembers: Receive = {
    case "get" =>
      context.sender() ! members
  }

  private def membersUpdates: Receive = {
    case MemberUp(member) =>
      updateMembers(members + ClusterMember(member))
//    case MemberRemoved(member) =>
//      members = members - ClusterMember(member)
    case MemberJoined(member) =>
      log.info(s"member joined $member")
  }

  private def leaderUpdates: Receive = {
    case LeaderChanged(address) =>
      setTheLeader(address)
      leaderAddress = address
  }

  def isLeader = leaderAddress.forall(address => address == Cluster(context.system).selfAddress )

  def setAsUnreachable(member: Member) = {
    updateMembers(members.map {
      clusterMember =>
        if (clusterMember.member == member) clusterMember.copy(unreachable = true)
        else clusterMember
    })
  }

  def setAsReachable(member: Member) = {
    updateMembers(members.map {
      clusterMember =>
        if (clusterMember.member == member) clusterMember.copy(unreachable = false)
        else clusterMember
    })
  }

  def noUnreachableMembers() = {
    members.forall( member => !member.unreachable )
  }

  def stopMember(member: Member) = {
    Cluster(context.system).down(member.address)
  }

}