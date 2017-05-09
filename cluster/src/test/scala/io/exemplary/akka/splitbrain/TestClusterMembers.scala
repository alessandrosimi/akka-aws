package io.exemplary.akka.splitbrain

import org.scalatest.concurrent.Eventually
import org.scalatest.{FeatureSpec, Matchers}

import scala.concurrent.Await
import scala.concurrent.duration._

/**
  * Test the population of the member
  * when they join a cluster
  */
class TestClusterMembers extends FeatureSpec with Matchers with Eventually {

  scenario("join") {
    val one = new Application(2551, List(2551))
    one.getClusterMembers should be (empty)
    val two = new Application(2552, List(2551))
    var members = Await.result(two.backdoor.getUpdatePromise.future, 10.seconds)
    members = Await.result(two.backdoor.getUpdatePromise.future, 30.seconds)
    two.getClusterMembers should have size 1
    one.shutdown
    two.shutdown
  }

}
