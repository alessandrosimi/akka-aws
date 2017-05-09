package io.exemplary.akka.splitbrain

import akka.cluster.Member

case class ClusterMember(member: Member,
                         instanceId: Option[AwsInstanceId] = None,
                         leader: Boolean = false,
                         unreachable: Boolean = false) {
  val address = member.address
}
