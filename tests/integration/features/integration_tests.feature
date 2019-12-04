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

    @1
    Scenario Outline: Perform a backup, verify it, and restore it
        Given I have a fresh ccm cluster running named "scenario1"
        Given I am using "<storage>" as storage provider
        When I create the "test" table in keyspace "medusa"
        When I load 100 rows in the "medusa.test" table
        When I run a "ccm node1 nodetool flush" command
        When I load 100 rows in the "medusa.test" table
        When I run a "ccm node1 nodetool flush" command
        When I perform a backup in "full" mode of the node named "first_backup"
        Then I can see the backup named "first_backup" when I list the backups
        Then I can see the backup status for "first_backup" when I run the status command
        Then backup named "first_backup" has 16 files in the manifest for the "test" table in keyspace "medusa"
        Then the backup index exists
        Then the backup named "first_backup" has 2 SSTables for the "test" table in keyspace "medusa"
        Then I can verify the backup named "first_backup" successfully
        When I load 100 rows in the "medusa.test" table
        When I run a "ccm node1 nodetool flush" command
        Then I have 300 rows in the "medusa.test" table
        When I restore the backup named "first_backup"
        Then I have 200 rows in the "medusa.test" table
        Examples: Storage
        | storage           |
        | local      |
#        | s3_us_west_oregon     |
#        | google_storage      |

    @2
    Scenario Outline: Perform a backup and verify its index
        Given I have a fresh ccm cluster running named "scenario2"
        Given I am using "<storage>" as storage provider
        When I create the "test" table in keyspace "medusa"
        When I load 100 rows in the "medusa.test" table
        When I run a "ccm node1 nodetool flush" command
        When I perform a backup in "full" mode of the node named "second_backup"
        Then the backup index exists
        Then I can see the backup index entry for "second_backup"
        Then I can see the latest backup for "localhost" being called "second_backup"
        When I perform a backup in "full" mode of the node named "third_backup"
        Then I can see the backup index entry for "second_backup"
        Then I can see the backup index entry for "third_backup"
        Then I can see the latest backup for "localhost" being called "third_backup"
        Then I can report latest backups without errors
        Examples: Storage
        | storage    |
        | local      |
#        | s3_us_west_oregon     |
#        | google_storage      |

    @3
    Scenario Outline: Perform a backup and verify the latest backup is updated correctly
        Given I have a fresh ccm cluster running named "scenario3"
        Given I am using "<storage>" as storage provider
        Then there is no latest backup for node "localhost"
        When I create the "test" table in keyspace "medusa"
        When I load 100 rows in the "medusa.test" table
        When I run a "ccm node1 nodetool flush" command
        When I perform a backup in "full" mode of the node named "fourth_backup"
        Then I can see the latest backup for "localhost" being called "fourth_backup"
        When I load 100 rows in the "medusa.test" table
        When I run a "ccm node1 nodetool flush" command
        When I perform a backup in "full" mode of the node named "fifth_backup"
        Then I can see the latest backup for "localhost" being called "fifth_backup"
        Examples: Storage
        | storage   |
        | local      |
#        | s3_us_west_oregon     |
#        | google_storage      |

    @4
    Scenario Outline: Perform a fake backup (by just writing an index) on different days and verify reports are correct
        Given I am using "<storage>" as storage provider
        When node "n1" fakes a complete backup named "backup1" on "2019-04-15 12:12:00"
        When node "n2" fakes a complete backup named "backup1" on "2019-04-15 12:14:00"
        Then the latest cluster backup is "backup1"
        Then there is no latest complete backup
        When I truncate the backup index
        Then the backup index does not exist
        When node "n1" fakes a complete backup named "backup1" on "2019-04-01 12:14:00"
        When node "n2" fakes a complete backup named "backup1" on "2019-04-01 12:16:00"
        When node "n1" fakes a complete backup named "backup2" on "2019-04-02 12:14:00"
        Then the backup index exists
        Then the latest cluster backup is "backup2"
        Then there is no latest complete backup
        When I truncate the backup index
        Then the backup index does not exist
        When node "n1" fakes a complete backup named "backup1" on "2019-04-01 12:14:00"
        When node "n2" fakes a complete backup named "backup1" on "2019-04-01 12:16:00"
        When node "n1" fakes a complete backup named "backup2" on "2019-04-02 12:14:00"
        When node "n2" fakes a complete backup named "backup3" on "2019-04-03 12:14:00"
        Then the backup index exists
        Then the latest cluster backup is "backup3"
        Then there is no latest complete backup
        When node "n2" fakes a complete backup named "backup2" on "2019-04-04 12:14:00"
        Then the latest cluster backup is "backup3"
        Then there is no latest complete backup
        When node "n1" fakes a complete backup named "backup3" on "2019-04-05 12:14:00"
        Then the latest cluster backup is "backup3"
        Then there is no latest complete backup
        When node "n3" fakes a complete backup named "backup2" on "2019-04-05 13:14:00"
        Then the latest cluster backup is "backup3"
        Then the latest complete cluster backup is "backup2"
        When node "n3" fakes a complete backup named "backup3" on "2019-04-05 14:14:00"
        Then the latest cluster backup is "backup3"
        Then the latest complete cluster backup is "backup3"
        Then I can report latest backups without errors
        Examples: Storage
        | storage    |
        | local      |
# other storage providers than local won't work with this test

    @5
    Scenario Outline: Verify re-creating index works
        Given I have a fresh ccm cluster running named "scenario5"
        Given I am using "<storage>" as storage provider
        When I create the "test" table in keyspace "medusa"
        When I load 100 rows in the "medusa.test" table
        When I run a "ccm node1 nodetool flush" command
        When I perform a backup in "full" mode of the node named "second_backup"
        When I perform a backup in "full" mode of the node named "third_backup"
        When I truncate the backup index
        Then the backup index does not exist
        Then there is no latest complete backup
        When I re-create the backup index
        Then the backup index exists
        Then the latest cluster backup is "third_backup"
        Then I can list and print backups without errors
        Then the latest complete cluster backup is "third_backup"
        Then I can report latest backups without errors
        Examples: Storage
        | storage   |
        | local      |
# other storage providers than local won't work with this test

    @6
    Scenario Outline: Verify that backups found only in index are silently ignored
        Given I have a fresh ccm cluster running named "scenario6"
        Given I am using "<storage>" as storage provider
        When I create the "test" table in keyspace "medusa"
        When I load 100 rows in the "medusa.test" table
        When I run a "ccm node1 nodetool flush" command
        When I perform a backup in "full" mode of the node named "first_backup"
        When I truncate the backup folder
        Then the backup index exists
        Then I can see no backups when I list the backups
        Examples: Storage
        | storage   |
        | local      |
# other storage providers than local won't work with this test

    @7
    Scenario Outline: Verify reporting metrics rebuilds the index if it is not present
        Given I have a fresh ccm cluster running named "scenario7"
        Given I am using "local" as storage provider
        When I create the "test" table in keyspace "medusa"
        When I load 100 rows in the "medusa.test" table
        When I run a "ccm node1 nodetool flush" command
        When I perform a backup in "full" mode of the node named "first_backup"
        Then the backup index exists
        When I truncate the backup index
        Then the backup index does not exist
        Then I can report latest backups without errors
        Then the backup index exists
        Examples:
        | Storage   |
        | local      |
# other storage providers than local won't work with this test

    @8
    Scenario Outline: Perform an differential backup, verify it, and restore it
        Given I have a fresh ccm cluster running named "scenario8"
        Given I am using "<storage>" as storage provider
        When I create the "test" table in keyspace "medusa"
        When I load 100 rows in the "medusa.test" table
        When I run a "ccm node1 nodetool flush" command
        When I perform a backup in "differential" mode of the node named "first_backup"
        Then I can see the backup named "first_backup" when I list the backups
        Then I can verify the backup named "first_backup" successfully
        Then backup named "first_backup" has 8 files in the manifest for the "test" table in keyspace "medusa"
        Then I can see 1 SSTables in the SSTable pool for the "test" table in keyspace "medusa"
        When I load 100 rows in the "medusa.test" table
        Then I have 200 rows in the "medusa.test" table
        When I run a "ccm node1 nodetool flush" command
        When I perform a backup in "differential" mode of the node named "second_backup"
        Then some files from the previous backup were not reuploaded
        Then I can see 2 SSTables in the SSTable pool for the "test" table in keyspace "medusa"
        Then I can see the backup named "second_backup" when I list the backups
        Then I can see the backup status for "second_backup" when I run the status command
        Then I can verify the backup named "second_backup" successfully
        Then backup named "first_backup" has 8 files in the manifest for the "test" table in keyspace "medusa"
        Then backup named "second_backup" has 16 files in the manifest for the "test" table in keyspace "medusa"
        When I load 100 rows in the "medusa.test" table
        When I run a "ccm node1 nodetool flush" command
        Then I have 300 rows in the "medusa.test" table
        When I perform a backup in "differential" mode of the node named "third_backup"
        Then I can see the backup named "third_backup" when I list the backups
        Then I can see the backup named "first_backup" when I list the backups
        Then I can see the backup named "second_backup" when I list the backups
        Then I can verify the backup named "third_backup" successfully
        When I restore the backup named "second_backup"
        Then I have 200 rows in the "medusa.test" table
        When I load 100 rows in the "medusa.test" table
        When I run a "ccm node1 nodetool flush" command
        Then I have 300 rows in the "medusa.test" table
        When I perform a backup in "differential" mode of the node named "fourth_backup"
        Then I can see the backup named "fourth_backup" when I list the backups
        Then I can see the backup named "first_backup" when I list the backups
        Then I can see the backup named "second_backup" when I list the backups
        Then I can see the backup named "third_backup" when I list the backups
        Then verify fails on the backup named "third_backup"
        Examples: Storage
        | storage   |
        | local      |
##        | google_storage      |
#        | s3_us_west_oregon     |

    @9
    Scenario Outline: Run a purge on backups
        Given I have a fresh ccm cluster running named "scenario9"
        Given I am using "<storage>" as storage provider
        When I create the "test" table in keyspace "medusa"
        When I load 100 rows in the "medusa.test" table
        When I run a "ccm node1 nodetool flush" command
        When I perform a backup in "differential" mode of the node named "first_backup"
        When I perform a backup in "differential" mode of the node named "second_backup"
        When I load 100 rows in the "medusa.test" table
        When I run a "ccm node1 nodetool flush" command
        When I run a "ccm node1 nodetool compact medusa" command
        When I load 100 rows in the "medusa.test" table
        When I run a "ccm node1 nodetool flush" command
        When I perform a backup in "differential" mode of the node named "third_backup"
        When I load 100 rows in the "medusa.test" table
        When I run a "ccm node1 nodetool flush" command
        When I perform a backup in "differential" mode of the node named "fourth_backup"
        When I run a "ccm node1 nodetool compact medusa" command
        When I perform a backup in "differential" mode of the node named "fifth_backup"
        Then I can see the backup named "first_backup" when I list the backups
        Then I can see the backup named "second_backup" when I list the backups
        Then I can see the backup named "third_backup" when I list the backups
        Then I can see the backup named "fourth_backup" when I list the backups
        Then I can see the backup named "fifth_backup" when I list the backups
        Then I can verify the backup named "fifth_backup" successfully
        When I purge the backup history to retain only 2 backups
        Then I cannot see the backup named "first_backup" when I list the backups
        Then I cannot see the backup named "second_backup" when I list the backups
        Then I cannot see the backup named "third_backup" when I list the backups
        Then I can see the backup named "fourth_backup" when I list the backups
        Then I can see the backup named "fifth_backup" when I list the backups
        Then I can verify the backup named "fourth_backup" successfully
        Then I can verify the backup named "fifth_backup" successfully
        Examples: Storage
        | storage   |
        | local      |
#        | google_storage      |
#        | s3_us_west_oregon     |

    @10
    Scenario Outline: Run a backup and restore and verify metrics
        Given I have a fresh ccm cluster running named "scenario10"
        Given I am using "<storage>" as storage provider
        When I create the "test" table in keyspace "medusa"
        When I load 100 rows in the "medusa.test" table
        When I run a "ccm node1 nodetool flush" command
        When I perform a backup in "differential" mode of the node named "first_backup"
        Then I see 3 metrics emitted
        Then I can report latest backups without errors
        Then I see 10 metrics emitted
        Examples: Storage
        | storage   |
        | local      |
#        | google_storage      |
#        | s3_us_west_oregon     |

    @11
    Scenario Outline: Perform a backup, and restore it using the sstableloader
        Given I have a fresh ccm cluster running named "scenario11"
        Given I am using "<storage>" as storage provider
        When I create the "test" table in keyspace "medusa"
        When I load 100 rows in the "medusa.test" table
        When I run a "ccm node1 nodetool flush" command
        When I load 100 rows in the "medusa.test" table
        When I run a "ccm node1 nodetool flush" command
        When I perform a backup in "full" mode of the node named "first_backup"
        Then I can see the backup named "first_backup" when I list the backups
        Then I can verify the backup named "first_backup" successfully
        When I load 100 rows in the "medusa.test" table
        When I run a "ccm node1 nodetool flush" command
        Then I have 300 rows in the "medusa.test" table
        When I truncate the "medusa.test" table
        When I restore the backup named "first_backup" with the sstableloader
        Then I have 200 rows in the "medusa.test" table
        Examples: Storage
        | storage   |
        | local      |
#        | s3_us_west_oregon     |
#        | google_storage      |

    @12
    Scenario Outline: Backup two tables but restore only one
        Given I have a fresh ccm cluster running named "scenario12"
        Given I am using "<storage>" as storage provider
        When I create the "test1" table in keyspace "medusa"
        When I load 100 rows in the "medusa.test1" table
        When I create the "test2" table in keyspace "medusa"
        When I load 100 rows in the "medusa.test2" table
        When I run a "ccm node1 nodetool flush" command
        Then I have 100 rows in the "medusa.test1" table
        Then I have 100 rows in the "medusa.test2" table
        When I perform a backup in "full" mode of the node named "first_backup"
        Then I can see the backup named "first_backup" when I list the backups
        Then I can see the backup status for "first_backup" when I run the status command
        Then I can verify the backup named "first_backup" successfully
        When I truncate the "medusa.test1" table
        When I truncate the "medusa.test2" table
        When I restore the backup named "first_backup" for "medusa.test2" table
        Then I have 100 rows in the "medusa.test2" table
        Then I have 0 rows in the "medusa.test1" table
        Examples: Storage
        | storage   |
        | local      |
#        | s3_us_west_oregon     |
#        | google_storage      |

    @13
    Scenario Outline: Perform a backup and a restore, then verify the restore
        Given I have a fresh ccm cluster running named "scenario13"
        Given I am using "<Storage>" as storage provider
        When I create the "test" table in keyspace "medusa"
        When I load 100 rows in the "medusa.test" table
        When I run a "ccm node1 nodetool flush" command
        When I perform a backup in "full" mode of the node named "first_backup"
        Then I can verify the backup named "first_backup" successfully
        When I restore the backup named "first_backup"
        Then I can verify the restore verify query "SELECT * FROM medusa.test" returned 100 rows

        Examples:
        | Storage   |
        | local      |
#        | s3_us_west_oregon     |
#        | google_storage      |

    @14
    Scenario Outline: Perform a backup & restore of a table with secondary index
        Given I have a fresh ccm cluster running named "scenario14"
        Given I am using "<Storage>" as storage provider
        When I create the "test" table with secondary index in keyspace "medusa"
        When I load 100 rows in the "medusa.test" table
        When I run a "ccm node1 nodetool flush" command
        When I perform a backup in "differential" mode of the node named "first_backup"
        Then I can verify the backup named "first_backup" successfully
        Then I can see secondary index files in the "first_backup" files
        When I restore the backup named "first_backup"
        Then I have 100 rows in the "medusa.test" table
        Then I can verify the restore verify query "SELECT * FROM medusa.test WHERE value = '0';" returned 1 rows

        Examples:
        | Storage   |
        | local      |
#        | s3_us_west_oregon     |
#        | google_storage      |
