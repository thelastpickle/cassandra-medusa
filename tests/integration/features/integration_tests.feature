# -*- coding: utf-8 -*-
# Copyright 2019 Spotify AB. All rights reserved.
#
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

Feature: Integration tests
    In order to run integration tests
    We'll spin up a Cassandra cluster

    Scenario Outline: Perform a backup, verify it, and restore it
        Given I have a fresh ccm cluster running named "scenario1"
        And I am using "<Storage>" as storage provider
        And I create the "test" table in keyspace "medusa"
        When I load "100" rows in the "medusa.test" table
        And run a "ccm node1 nodetool flush" command
        And I load "100" rows in the "medusa.test" table
        And run a "ccm node1 nodetool flush" command
        And I perform a backup in "full" mode of the node named "first_backup"
        Then I can see the backup named "first_backup" when I list the backups
        And I can see the backup status for "first_backup" when I run the status command
        And backup named "first_backup" has 16 files in the manifest for the "test" table in keyspace "medusa"
        And the backup index exists
        And the backup named "first_backup" has 2 SSTables for the "test" table in keyspace "medusa"
        And I can verify the backup named "first_backup" successfully
        When I load "100" rows in the "medusa.test" table
        And run a "ccm node1 nodetool flush" command
        Given I have "300" rows in the "medusa.test" table
        When I restore the backup named "first_backup"
        Then I have "200" rows in the "medusa.test" table

        Examples:
        | Storage   |
        | local     |
#        | s3_us_west_oregon     |
#        | google_storage      |

    Scenario Outline: Perform a backup and verify its index
        Given I have a fresh ccm cluster running named "scenario2"
        And I am using "local" as storage provider
        And I create the "test" table in keyspace "medusa"
        When I load "100" rows in the "medusa.test" table
        And run a "ccm node1 nodetool flush" command
        And I perform a backup in "full" mode of the node named "second_backup"
        Then the backup index exists
        And I can see the backup index entry for "second_backup"
        And I can see the latest backup for "localhost" being called "second_backup"
        Given I perform a backup in "full" mode of the node named "third_backup"
        Then I can see the backup index entry for "second_backup"
        Then I can see the backup index entry for "third_backup"
        And I can see the latest backup for "localhost" being called "third_backup"
        And I can report latest backups without errors

        Examples:
        | Storage   |
        | local      |
#        | s3_us_west_oregon     |
#        | google_storage      |

    Scenario Outline: Perform a backup and verify the latest backup is updated correctly
        Given I have a fresh ccm cluster running named "scenario3"
        And I am using "<Storage>" as storage provider
        Then there is no latest backup for node "localhost"
        Given I create the "test" table in keyspace "medusa"
        When I load "100" rows in the "medusa.test" table
        And run a "ccm node1 nodetool flush" command
        And I perform a backup in "full" mode of the node named "fourth_backup"
        And I can see the latest backup for "localhost" being called "fourth_backup"
        When I load "100" rows in the "medusa.test" table
        And run a "ccm node1 nodetool flush" command
        And I perform a backup in "full" mode of the node named "fifth_backup"
        And I can see the latest backup for "localhost" being called "fifth_backup"

        Examples:
        | Storage   |
        | local      |
#        | s3_us_west_oregon     |
#        | google_storage      |

    Scenario Outline: Perform a fake backup (by just writing an index) on different days and verify reports are correct
        Given I am using "<Storage>" as storage provider
        And node "n1" fakes a complete backup named "backup1" on "2019-04-15 12:12:00"
        And node "n2" fakes a complete backup named "backup1" on "2019-04-15 12:14:00"
        Then the latest cluster backup is "backup1"
        And there is no latest complete backup
        When I truncate the backup index
        Then the backup index does not exist
        When node "n1" fakes a complete backup named "backup1" on "2019-04-01 12:14:00"
        And node "n2" fakes a complete backup named "backup1" on "2019-04-01 12:16:00"
        And node "n1" fakes a complete backup named "backup2" on "2019-04-02 12:14:00"
        Then the backup index exists
        And the latest cluster backup is "backup2"
        And there is no latest complete backup
        When I truncate the backup index
        Then the backup index does not exist
        When node "n1" fakes a complete backup named "backup1" on "2019-04-01 12:14:00"
        And node "n2" fakes a complete backup named "backup1" on "2019-04-01 12:16:00"
        And node "n1" fakes a complete backup named "backup2" on "2019-04-02 12:14:00"
        And node "n2" fakes a complete backup named "backup3" on "2019-04-03 12:14:00"
        Then the backup index exists
        And the latest cluster backup is "backup3"
        And there is no latest complete backup
        When node "n2" fakes a complete backup named "backup2" on "2019-04-04 12:14:00"
        Then the latest cluster backup is "backup3"
        And there is no latest complete backup
        When node "n1" fakes a complete backup named "backup3" on "2019-04-05 12:14:00"
        Then the latest cluster backup is "backup3"
        And there is no latest complete backup
        When node "n3" fakes a complete backup named "backup2" on "2019-04-05 13:14:00"
        Then the latest cluster backup is "backup3"
        And the latest complete cluster backup is "backup2"
        Then node "n3" fakes a complete backup named "backup3" on "2019-04-05 14:14:00"
        Then the latest cluster backup is "backup3"
        And the latest complete cluster backup is "backup3"
        And I can report latest backups without errors

        Examples:
        | Storage   |
        | local      |
# other storage providers than local won't work with this test

    Scenario Outline: Verify re-creating index works
        Given I have a fresh ccm cluster running named "scenario4"
        And I am using "<Storage>" as storage provider
        And I create the "test" table in keyspace "medusa"
        When I load "100" rows in the "medusa.test" table
        And run a "ccm node1 nodetool flush" command
        And I perform a backup in "full" mode of the node named "second_backup"
        And I perform a backup in "full" mode of the node named "third_backup"
        And I truncate the backup index
        Then the backup index does not exist
        And there is no latest complete backup
        When I re-create the backup index
        Then the backup index exists
        And the latest cluster backup is "third_backup"
        And I can list and print backups without errors
        And the latest complete cluster backup is "third_backup"
        And I can report latest backups without errors

        Examples:
        | Storage   |
        | local      |
# other storage providers than local won't work with this test

    Scenario Outline: Verify that backups found only in index are silently ignored
        Given I have a fresh ccm cluster running named "scenario6"
        And I am using "local" as storage provider
        And I create the "test" table in keyspace "medusa"
        When I load "100" rows in the "medusa.test" table
        And run a "ccm node1 nodetool flush" command
        And I perform a backup in "full" mode of the node named "first_backup"
        When I truncate the backup folder
        Then the backup index exists
        And I can see no backups when I list the backups

        Examples:
        | Storage   |
        | local      |
# other storage providers than local won't work with this test


    Scenario Outline: Verify reporting metrics rebuilds the index if it is not present
        Given I have a fresh ccm cluster running named "scenario7"
        And I am using "local" as storage provider
        And I create the "test" table in keyspace "medusa"
        When I load "100" rows in the "medusa.test" table
        And run a "ccm node1 nodetool flush" command
        And I perform a backup in "full" mode of the node named "first_backup"
        Then the backup index exists
        When I truncate the backup index
        Then the backup index does not exist
        When I can report latest backups without errors
        Then the backup index exists

        Examples:
        | Storage   |
        | local      |
# other storage providers than local won't work with this test

    Scenario Outline: Perform an incremental backup, verify it, and restore it
        Given I have a fresh ccm cluster running named "scenario8"
        And I am using "<Storage>" as storage provider
        And I create the "test" table in keyspace "medusa"
        When I load "100" rows in the "medusa.test" table
        And run a "ccm node1 nodetool flush" command
        And I perform a backup in "incremental" mode of the node named "first_backup"
        Then I can see the backup named "first_backup" when I list the backups
        And I can verify the backup named "first_backup" successfully
        And backup named "first_backup" has 8 files in the manifest for the "test" table in keyspace "medusa"
        And I can see 1 SSTable in the SSTable pool for the "test" table in keyspace "medusa"
        When I load "100" rows in the "medusa.test" table
        Then I have "200" rows in the "medusa.test" table
        And run a "ccm node1 nodetool flush" command
        And I perform a backup in "incremental" mode of the node named "second_backup"
        Then I can see 2 SSTables in the SSTable pool for the "test" table in keyspace "medusa"
        And I can see the backup named "second_backup" when I list the backups
        And I can see the backup status for "second_backup" when I run the status command
        And I can verify the backup named "second_backup" successfully
        And backup named "first_backup" has 8 files in the manifest for the "test" table in keyspace "medusa"
        And backup named "second_backup" has 16 files in the manifest for the "test" table in keyspace "medusa"
        When I load "100" rows in the "medusa.test" table
        And run a "ccm node1 nodetool flush" command
        Then I have "300" rows in the "medusa.test" table
        Given I perform a backup in "incremental" mode of the node named "third_backup"
        Then I can see the backup named "third_backup" when I list the backups
        And I can see the backup named "first_backup" when I list the backups
        And I can see the backup named "second_backup" when I list the backups
        And I can verify the backup named "third_backup" successfully
        When I restore the backup named "second_backup"
        Then I have "200" rows in the "medusa.test" table
        When I load "100" rows in the "medusa.test" table
        And run a "ccm node1 nodetool flush" command
        Then I have "300" rows in the "medusa.test" table
        Given I perform a backup in "incremental" mode of the node named "fourth_backup"
        Then I can see the backup named "fourth_backup" when I list the backups
        And I can see the backup named "first_backup" when I list the backups
        And I can see the backup named "second_backup" when I list the backups
        And I can see the backup named "third_backup" when I list the backups
        And verify fails on the backup named "third_backup"

        Examples:
        | Storage   |
        | local      |
#        | google_storage      |
#        | s3_us_west_oregon      |

    Scenario Outline: Run a purge on backups
        Given I have a fresh ccm cluster running named "scenario9"
        And I am using "<Storage>" as storage provider
        And I create the "test" table in keyspace "medusa"
        When I load "100" rows in the "medusa.test" table
        And run a "ccm node1 nodetool flush" command
        And I perform a backup in "incremental" mode of the node named "first_backup"
        And I perform a backup in "incremental" mode of the node named "second_backup"
        When I load "100" rows in the "medusa.test" table
        And run a "ccm node1 nodetool flush" command
        And run a "ccm node1 nodetool compact medusa" command
        When I load "100" rows in the "medusa.test" table
        And run a "ccm node1 nodetool flush" command
        And I perform a backup in "incremental" mode of the node named "third_backup"
        When I load "100" rows in the "medusa.test" table
        And run a "ccm node1 nodetool flush" command
        And I perform a backup in "incremental" mode of the node named "fourth_backup"
        And run a "ccm node1 nodetool compact medusa" command
        And I perform a backup in "incremental" mode of the node named "fifth_backup"
        Then I can see the backup named "first_backup" when I list the backups
        Then I can see the backup named "second_backup" when I list the backups
        Then I can see the backup named "third_backup" when I list the backups
        Then I can see the backup named "fourth_backup" when I list the backups
        Then I can see the backup named "fifth_backup" when I list the backups
        And I can verify the backup named "fifth_backup" successfully
        When I purge the backup history to retain only 2 backups
        Then I cannot see the backup named "first_backup" when I list the backups
        Then I cannot see the backup named "second_backup" when I list the backups
        Then I cannot see the backup named "third_backup" when I list the backups
        Then I can see the backup named "fourth_backup" when I list the backups
        Then I can see the backup named "fifth_backup" when I list the backups
        And I can verify the backup named "fourth_backup" successfully
        And I can verify the backup named "fifth_backup" successfully

        Examples:
        | Storage   |
        | local      |
#        | google_storage      |
#        | s3_us_west_oregon      |

    Scenario Outline: Run a backup and restore and verify metrics
        Given I have a fresh ccm cluster running named "scenario10"
        And I am using "<Storage>" as storage provider
        And I create the "test" table in keyspace "medusa"
        When I load "100" rows in the "medusa.test" table
        And run a "ccm node1 nodetool flush" command
        And I perform a backup in "incremental" mode of the node named "first_backup"
        Then I see "3" metrics emitted
        When I can report latest backups without errors
        Then I see "10" metrics emitted

        Examples:
        | Storage   |
        | local      |
#        | google_storage      |
#        | s3_us_west_oregon      |

    Scenario Outline: Perform a backup, and restore it using the sstableloader
        Given I have a fresh ccm cluster running named "scenario1"
        And I am using "<Storage>" as storage provider
        And I create the "test" table in keyspace "medusa"
        When I load "100" rows in the "medusa.test" table
        And run a "ccm node1 nodetool flush" command
        And I load "100" rows in the "medusa.test" table
        And run a "ccm node1 nodetool flush" command
        And I perform a backup in "full" mode of the node named "first_backup"
        Then I can see the backup named "first_backup" when I list the backups
        And I can verify the backup named "first_backup" successfully
        When I load "100" rows in the "medusa.test" table
        And run a "ccm node1 nodetool flush" command
        Given I have "300" rows in the "medusa.test" table
        And I truncate the "medusa.test" table
        When I restore the backup named "first_backup" with the sstableloader
        Then I have "200" rows in the "medusa.test" table

        Examples:
        | Storage   |
        | local     |
#        | s3_us_west_oregon     |
#        | google_storage      |