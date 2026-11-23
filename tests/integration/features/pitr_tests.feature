# -*- coding: utf-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

Feature: Point-in-time restore integration tests
    In order to check the point-in-time restore end to end
    We'll spin up a Cassandra cluster, archive its commitlogs and replay them up to a target timestamp

    # The pauses keep the target timestamp at least 2s away from the backup and from the last writes:
    # the snapshot_time of a backup has a resolution of 1s and a base backup is only eligible if
    # snapshot_time + 1s <= the target.
    # Dropping the node's commitlog segments into the archive needs a clean stop of the node, otherwise the segment
    # that the last writes went to is still being written to, and nothing proves that the writes after the target
    # were archived (an empty spool says nothing).
    @pitr
    Scenario Outline: Restore a node to a point in time between a backup and the end of the workload
        Given I have a fresh ccm cluster "<client encryption>" running named "scenario_pitr"
        Given I am using "<storage>" as storage provider in ccm cluster "<client encryption>"
        Given PITR commitlog archiving and a gRPC server are enabled on the ccm node
        When I create the "test" table in keyspace "medusa"
        When I load 100 rows in the "medusa.test" table
        When I run a "ccm node1 nodetool -- -Dcom.sun.jndi.rmiURLParsing=legacy flush" command
        When I perform a backup in "full" mode of the node named "pitr_base" with md5 checks "disabled"
        Then I wait for 2 seconds
        When I load 100 rows in the "medusa.test" table
        Then I wait for 2 seconds
        When I record the PITR target timestamp and the number of rows in the "medusa.test" table
        Then I wait for 2 seconds
        When I load 100 rows in the "medusa.test" table
        When I force the commitlog segments to be sealed and archived by restarting the node
        Then the commitlog archiver uploads everything that was sealed
        When I prepare a PITR restore to the recorded target timestamp for the datacenter "datacenter1"
        When I restore the node to the recorded PITR target the way the Kubernetes init containers do
        Then I have the number of rows recorded for the PITR target in the "medusa.test" table

        @local
        Examples: Local storage
        | storage | client encryption          |
        | local   | without_client_encryption  |
