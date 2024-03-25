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
    Scenario Outline: Perform a backup, verify it, and restore it.
        Given I have a fresh ccm cluster "<client encryption>" running named "scenario1"
        Then Test TLS version connections if "<client encryption>" is turned on
        Given I am using "<storage>" as storage provider in ccm cluster "<client encryption>"
        When I create the "test" table with secondary index in keyspace "medusa"
        When I load 100 rows in the "medusa.test" table
        When I run a "ccm node1 nodetool -- -Dcom.sun.jndi.rmiURLParsing=legacy flush" command
        When I load 100 rows in the "medusa.test" table
        When I run a "ccm node1 nodetool -- -Dcom.sun.jndi.rmiURLParsing=legacy flush" command
        When I perform a backup in "full" mode of the node named "first_backup" with md5 checks "disabled"
        Then I can see the backup named "first_backup" when I list the backups
        And the backup "first_backup" has server_type "cassandra" in its metadata
        And all files of "medusa.test" in "first_backup" were uploaded with KMS key as configured in "<storage>"
        Then I can download the backup named "first_backup" for all tables
        Then I can download the backup named "first_backup" for "medusa.test"
        And I can fetch the tokenmap of the backup named "first_backup"
        And the schema of the backup named "first_backup" was uploaded with KMS key according to "<storage>"
        Then I can see the backup status for "first_backup" when I run the status command
        Then backup named "first_backup" has 32 files in the manifest for the "test" table in keyspace "medusa"
        Then the backup index exists
        Then the backup named "first_backup" has 4 SSTables for the "test" table in keyspace "medusa"
        Then I can verify the backup named "first_backup" with md5 checks "disabled" successfully
        Then I can verify the backup named "first_backup" with md5 checks "enabled" successfully
        When I load 100 rows in the "medusa.test" table
        When I run a "ccm node1 nodetool -- -Dcom.sun.jndi.rmiURLParsing=legacy flush" command
        Then I have 300 rows in the "medusa.test" table in ccm cluster "<client encryption>"
        When I restore the backup named "first_backup"
        Then I have 200 rows in the "medusa.test" table in ccm cluster "<client encryption>"

        @local
        Examples: Local storage
        | storage           | client encryption |
        | local      | with_client_encryption |

        @s3
        Examples: S3 storage
        | storage           | client encryption |
        | s3_us_west_oregon     | without_client_encryption |
        | s3_us_west_oregon_encrypted     | without_client_encryption |

        @gcs
        Examples: Google Cloud Storage
        | storage           | client encryption |
        | google_storage      | without_client_encryption |

        @azure
        Examples: Azure Blob Storage
        | storage           | client encryption |
        | azure_blobs      | without_client_encryption |
        
        @ibm
        Examples: IBM Cloud Object Storage
        | storage           | client encryption |
        | ibm_storage      | without_client_encryption |

        @minio
        Examples: MinIO storage
        | storage           | client encryption         |
        | minio             | without_client_encryption |

    @2
    Scenario Outline: Perform a backup and verify its index
        Given I have a fresh ccm cluster "<client encryption>" running named "scenario2"
        Given I am using "<storage>" as storage provider in ccm cluster "<client encryption>"
        When I create the "test" table in keyspace "medusa"
        When I load 100 rows in the "medusa.test" table
        When I run a "ccm node1 nodetool -- -Dcom.sun.jndi.rmiURLParsing=legacy flush" command
        When I perform a backup in "full" mode of the node named "second_backup" with md5 checks "disabled"
        Then the backup index exists
        Then I can see the backup index entry for "second_backup"
        Then I can see the latest backup for "127.0.0.1" being called "second_backup"
        When I perform a backup in "full" mode of the node named "third_backup" with md5 checks "disabled"
        Then I can see the backup index entry for "second_backup"
        Then I can see the backup index entry for "third_backup"
        Then I can see the latest backup for "127.0.0.1" being called "third_backup"
        Then I can report latest backups without errors
        When I perform a backup in "full" mode of the node named "fourth_backup" with md5 checks "enabled"
        Then I can report latest backups without errors
        @local
        Examples: Local storage
        | storage           | client encryption |
        | local      |  with_client_encryption |

        @s3
        Examples: S3 storage
        | storage           | client encryption |
        | s3_us_west_oregon     |  without_client_encryption |

        @gcs
        Examples: Google Cloud Storage
        | storage           | client encryption |
        | google_storage      |  without_client_encryption |

        @azure
        Examples: Azure Blob Storage
        | storage           | client encryption |
        | azure_blobs      | without_client_encryption |
        
        @ibm
        Examples: IBM Cloud Object Storage
        | storage           | client encryption |
        | ibm_storage      | without_client_encryption |

        @minio
        Examples: MinIO storage
        | storage           | client encryption         |
        | minio             | without_client_encryption |

    @3
    Scenario Outline: Perform a backup and verify the latest backup is updated correctly
        Given I have a fresh ccm cluster "<client encryption>" running named "scenario3"
        Given I am using "<storage>" as storage provider in ccm cluster "<client encryption>"
        Then there is no latest backup for node "127.0.0.1"
        When I create the "test" table in keyspace "medusa"
        When I load 100 rows in the "medusa.test" table
        When I run a "ccm node1 nodetool -- -Dcom.sun.jndi.rmiURLParsing=legacy flush" command
        When I perform a backup in "full" mode of the node named "fourth_backup" with md5 checks "disabled"
        Then I can see the latest backup for "127.0.0.1" being called "fourth_backup"
        When I load 100 rows in the "medusa.test" table
        When I run a "ccm node1 nodetool -- -Dcom.sun.jndi.rmiURLParsing=legacy flush" command
        When I perform a backup in "full" mode of the node named "fifth_backup" with md5 checks "disabled"
        Then I can see the latest backup for "127.0.0.1" being called "fifth_backup"
        @local
        Examples: Local storage
        | storage           | client encryption |
        | local      |  with_client_encryption |

        @s3
        Examples: S3 storage
        | storage           | client encryption |
        | s3_us_west_oregon     |  without_client_encryption |

        @gcs
        Examples: Google Cloud Storage
        | storage           | client encryption |
        | google_storage      |  without_client_encryption |

        @azure
        Examples: Azure Blob Storage
        | storage           | client encryption |
        | azure_blobs      | without_client_encryption |
        
        @ibm
        Examples: IBM Cloud Object Storage
        | storage           | client encryption |
        | ibm_storage      | without_client_encryption |

        @minio
        Examples: MinIO storage
        | storage           | client encryption         |
        | minio             | without_client_encryption |

    @4
    Scenario Outline: Perform a fake backup (by just writing an index) on different days and verify reports are correct
        Given I am using "<storage>" as storage provider in ccm cluster "<client encryption>"
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

        @local
        Examples: Local storage
        | storage           | client encryption |
        | local      |  with_client_encryption |


# other storage providers than local won't work with this test

    @5
    Scenario Outline: Verify re-creating index works
        Given I have a fresh ccm cluster "<client encryption>" running named "scenario5"
        Given I am using "<storage>" as storage provider in ccm cluster "<client encryption>"
        When I create the "test" table in keyspace "medusa"
        When I load 100 rows in the "medusa.test" table
        When I run a "ccm node1 nodetool -- -Dcom.sun.jndi.rmiURLParsing=legacy flush" command
        When I perform a backup in "full" mode of the node named "second_backup" with md5 checks "disabled"
        When I perform a backup in "full" mode of the node named "third_backup" with md5 checks "disabled"
        When I truncate the backup index
        Then the backup index does not exist
        Then there is no latest complete backup
        When I re-create the backup index
        Then the backup index exists
        Then the latest cluster backup is "third_backup"
        Then I can list and print backups without errors
        Then the latest complete cluster backup is "third_backup"
        Then I can report latest backups without errors

        @local
        Examples: Local storage
        | storage           | client encryption |
        | local      |  with_client_encryption |
# other storage providers than local won't work with this test

    @6
    Scenario Outline: Verify that backups found only in index are silently ignored
        Given I have a fresh ccm cluster "<client encryption>" running named "scenario6"
        Given I am using "<storage>" as storage provider in ccm cluster "<client encryption>"
        When I create the "test" table in keyspace "medusa"
        When I load 100 rows in the "medusa.test" table
        When I run a "ccm node1 nodetool -- -Dcom.sun.jndi.rmiURLParsing=legacy flush" command
        When I perform a backup in "full" mode of the node named "first_backup" with md5 checks "disabled"
        When I truncate the backup folder
        Then the backup index exists
        Then I can see no backups when I list the backups

        @local
        Examples: Local storage
        | storage           | client encryption |
        | local      |  with_client_encryption |
# other storage providers than local won't work with this test

    @7
    Scenario Outline: Verify reporting metrics rebuilds the index if it is not present
        Given I have a fresh ccm cluster "<client encryption>" running named "scenario7"
        Given I am using "local" as storage provider in ccm cluster "<client encryption>"
        When I create the "test" table in keyspace "medusa"
        When I load 100 rows in the "medusa.test" table
        When I run a "ccm node1 nodetool -- -Dcom.sun.jndi.rmiURLParsing=legacy flush" command
        When I perform a backup in "full" mode of the node named "first_backup" with md5 checks "disabled"
        Then the backup index exists
        When I truncate the backup index
        Then the backup index does not exist
        Then I can report latest backups without errors
        Then the backup index exists

        @local
        Examples: Local storage
        | storage           | client encryption |
        | local      |  with_client_encryption |
# other storage providers than local won't work with this test

    @8
    Scenario Outline: Perform an differential backup, verify it, restore it and delete it
        Given I have a fresh ccm cluster "<client encryption>" running named "scenario8"
        Given I am using "<storage>" as storage provider in ccm cluster "<client encryption>"
        When I create the "test" table with secondary index in keyspace "medusa"
        When I load 100 rows in the "medusa.test" table
        When I run a "ccm node1 nodetool -- -Dcom.sun.jndi.rmiURLParsing=legacy flush" command
        When I perform a backup in "differential" mode of the node named "first_backup" with md5 checks "disabled"
        Then I can see the backup named "first_backup" when I list the backups
        Then I can verify the backup named "first_backup" with md5 checks "disabled" successfully
        Then backup named "first_backup" has 16 files in the manifest for the "test" table in keyspace "medusa"
        Then I can see 2 SSTables in the SSTable pool for the "test" table in keyspace "medusa"
        When I load 100 rows in the "medusa.test" table
        Then I have 200 rows in the "medusa.test" table in ccm cluster "<client encryption>"
        When I run a "ccm node1 nodetool -- -Dcom.sun.jndi.rmiURLParsing=legacy flush" command
        When I perform a backup in "differential" mode of the node named "second_backup" with md5 checks "disabled"
        Then some files from the previous backup were not reuploaded
        Then I can see 4 SSTables in the SSTable pool for the "test" table in keyspace "medusa"
        Then I can see the backup named "second_backup" when I list the backups
        Then I can see the backup status for "second_backup" when I run the status command
        Then I can verify the backup named "second_backup" with md5 checks "disabled" successfully
        Then backup named "first_backup" has 16 files in the manifest for the "test" table in keyspace "medusa"
        Then backup named "second_backup" has 32 files in the manifest for the "test" table in keyspace "medusa"
        When I perform a backup in "differential" mode of the node named "third_backup" with md5 checks "enabled"
        When I load 100 rows in the "medusa.test" table
        When I run a "ccm node1 nodetool -- -Dcom.sun.jndi.rmiURLParsing=legacy flush" command
        Then I have 300 rows in the "medusa.test" table in ccm cluster "<client encryption>"
        When I perform a backup in "differential" mode of the node named "fourth_backup" with md5 checks "enabled"
        Then some files from the previous backup were not reuploaded
        Then I can see the backup named "third_backup" when I list the backups
        Then I can see the backup named "first_backup" when I list the backups
        Then I can see the backup named "second_backup" when I list the backups
        Then I can verify the backup named "third_backup" with md5 checks "disabled" successfully
        When I restore the backup named "third_backup"
        Then I have 200 rows in the "medusa.test" table in ccm cluster "<client encryption>"
        When I load 100 rows in the "medusa.test" table
        When I run a "ccm node1 nodetool -- -Dcom.sun.jndi.rmiURLParsing=legacy flush" command
        Then I have 300 rows in the "medusa.test" table in ccm cluster "<client encryption>"
        When I perform a backup in "differential" mode of the node named "fifth_backup" with md5 checks "disabled"
        Then I can see the backup named "fourth_backup" when I list the backups
        Then I can see the backup named "first_backup" when I list the backups
        Then I can see the backup named "second_backup" when I list the backups
        Then I can see the backup named "third_backup" when I list the backups
        # The following is disabled as we don't compare hashes with local storage
        # Then verify fails on the backup named "third_backup"
        When I delete the backup named "first_backup"
        Then I cannot see the backup named "first_backup" when I list the backups
        Then I can see the backup named "second_backup" when I list the backups
        Then I can see the backup named "third_backup" when I list the backups

        @local
        Examples: Local storage
        | storage           | client encryption |
        | local      |  with_client_encryption |

        @s3
        Examples: S3 storage
        | storage           | client encryption |
        | s3_us_west_oregon     |  without_client_encryption |

        @gcs
        Examples: Google Cloud Storage
        | storage           | client encryption |
        | google_storage      |  without_client_encryption |

        @azure
        Examples: Azure Blob Storage
        | storage           | client encryption |
        | azure_blobs      | without_client_encryption |
        
        @ibm
        Examples: IBM Cloud Object Storage
        | storage           | client encryption |
        | ibm_storage      | without_client_encryption |

        @minio
        Examples: MinIO storage
        | storage           | client encryption         |
        | minio             | without_client_encryption |

    @9
    Scenario Outline: Run a purge on backups
        Given I have a fresh ccm cluster "<client encryption>" running named "scenario9"
        Given I am using "<storage>" as storage provider in ccm cluster "<client encryption>"
        When I create the "test" table in keyspace "medusa"
        When I load 100 rows in the "medusa.test" table
        When I run a "ccm node1 nodetool -- -Dcom.sun.jndi.rmiURLParsing=legacy flush" command
        When I perform a backup in "differential" mode of the node named "first_backup" with md5 checks "disabled"
        When I perform a backup in "differential" mode of the node named "second_backup" with md5 checks "disabled"
        When I load 100 rows in the "medusa.test" table
        When I run a "ccm node1 nodetool -- -Dcom.sun.jndi.rmiURLParsing=legacy flush" command
        When I run a "ccm node1 nodetool -- -Dcom.sun.jndi.rmiURLParsing=legacy compact medusa" command
        When I load 100 rows in the "medusa.test" table
        When I run a "ccm node1 nodetool -- -Dcom.sun.jndi.rmiURLParsing=legacy flush" command
        When I perform a backup in "differential" mode of the node named "third_backup" with md5 checks "disabled"
        When I load 100 rows in the "medusa.test" table
        When I run a "ccm node1 nodetool -- -Dcom.sun.jndi.rmiURLParsing=legacy flush" command
        When I perform a backup in "differential" mode of the node named "fourth_backup" with md5 checks "disabled"
        When I run a "ccm node1 nodetool -- -Dcom.sun.jndi.rmiURLParsing=legacy compact medusa" command
        When I perform a backup in "differential" mode of the node named "fifth_backup" with md5 checks "disabled"
        Then I can see the backup named "first_backup" when I list the backups
        Then I can see the backup named "second_backup" when I list the backups
        Then I can see the backup named "third_backup" when I list the backups
        Then I can see the backup named "fourth_backup" when I list the backups
        Then I can see the backup named "fifth_backup" when I list the backups
        Then I can verify the backup named "fifth_backup" with md5 checks "disabled" successfully
        When I purge the backup history to retain only 2 backups
        Then I cannot see the backup named "first_backup" when I list the backups
        Then I cannot see the backup named "second_backup" when I list the backups
        Then I cannot see the backup named "third_backup" when I list the backups
        Then I can not see purged backup files for the "test" table in keyspace "medusa"
        Then I can see the backup named "fourth_backup" when I list the backups
        Then I can see the backup named "fifth_backup" when I list the backups
        Then I can verify the backup named "fourth_backup" with md5 checks "disabled" successfully
        Then I can verify the backup named "fifth_backup" with md5 checks "disabled" successfully

        @local
        Examples: Local storage
        | storage           | client encryption |
        | local      |  with_client_encryption |

        @s3
        Examples: S3 storage
        | storage           | client encryption |
        | s3_us_west_oregon     |  without_client_encryption |

        @gcs
        Examples: Google Cloud Storage
        | storage           | client encryption |
        | google_storage      |  without_client_encryption |

        @azure
        Examples: Azure Blob Storage
        | storage           | client encryption |
        | azure_blobs      | without_client_encryption |
        
        @ibm
        Examples: IBM Cloud Object Storage
        | storage           | client encryption |
        | ibm_storage      | without_client_encryption |

        @minio
        Examples: MinIO storage
        | storage           | client encryption         |
        | minio             | without_client_encryption |

    @10
    Scenario Outline: Run a backup and restore and verify metrics
        Given I have a fresh ccm cluster "<client encryption>" running named "scenario10"
        Given I am using "<storage>" as storage provider in ccm cluster "<client encryption>"
        When I create the "test" table in keyspace "medusa"
        When I load 100 rows in the "medusa.test" table
        When I run a "ccm node1 nodetool -- -Dcom.sun.jndi.rmiURLParsing=legacy flush" command
        When I perform a backup in "differential" mode of the node named "first_backup" with md5 checks "disabled"
        Then I see 3 metrics emitted
        Then I can report latest backups without errors
        Then I see 10 metrics emitted

        @local
        Examples: Local storage
        | storage           | client encryption |
        | local      |  with_client_encryption |

        @s3
        Examples: S3 storage
        | storage           | client encryption |
        | s3_us_west_oregon     |  without_client_encryption |

        @gcs
        Examples: Google Cloud Storage
        | storage           | client encryption |
        | google_storage      |  without_client_encryption |

        @azure
        Examples: Azure Blob Storage
        | storage           | client encryption |
        | azure_blobs      | without_client_encryption |
        
        @ibm
        Examples: IBM Cloud Object Storage
        | storage           | client encryption |
        | ibm_storage      | without_client_encryption |

        @minio
        Examples: MinIO storage
        | storage           | client encryption         |
        | minio             | without_client_encryption |

    @11
    Scenario Outline: Perform a backup, and restore it using the sstableloader
        Given I have a fresh ccm cluster "<client encryption>" running named "scenario11"
        Given I am using "<storage>" as storage provider in ccm cluster "<client encryption>"
        When I create the "test" table in keyspace "medusa"
        When I load 100 rows in the "medusa.test" table
        When I run a "ccm node1 nodetool -- -Dcom.sun.jndi.rmiURLParsing=legacy flush" command
        When I load 100 rows in the "medusa.test" table
        When I run a "ccm node1 nodetool -- -Dcom.sun.jndi.rmiURLParsing=legacy flush" command
        When I perform a backup in "full" mode of the node named "first_backup" with md5 checks "disabled"
        Then I can see the backup named "first_backup" when I list the backups
        Then I can verify the backup named "first_backup" with md5 checks "disabled" successfully
        When I load 100 rows in the "medusa.test" table
        When I run a "ccm node1 nodetool -- -Dcom.sun.jndi.rmiURLParsing=legacy flush" command
        Then I have 300 rows in the "medusa.test" table in ccm cluster "<client encryption>"
        When I truncate the "medusa.test" table in ccm cluster "<client encryption>"
        When I restore the backup named "first_backup" with the sstableloader
        Then I have 200 rows in the "medusa.test" table in ccm cluster "<client encryption>"

        @local
        Examples: Local storage
        | storage           | client encryption |
        | local      |  with_client_encryption |

        @s3
        Examples: S3 storage
        | storage           | client encryption |
        | s3_us_west_oregon     |  without_client_encryption |

        @gcs
        Examples: Google Cloud Storage
        | storage           | client encryption |
        | google_storage      |  without_client_encryption |

        @azure
        Examples: Azure Blob Storage
        | storage           | client encryption |
        | azure_blobs      | without_client_encryption |
        
        @ibm
        Examples: IBM Cloud Object Storage
        | storage           | client encryption |
        | ibm_storage      | without_client_encryption |

        @minio
        Examples: MinIO storage
        | storage           | client encryption         |
        | minio             | without_client_encryption |

    @12
    Scenario Outline: Backup two tables but restore only one
        Given I have a fresh ccm cluster "<client encryption>" running named "scenario12"
        Given I am using "<storage>" as storage provider in ccm cluster "<client encryption>"
        When I create the "test1" table in keyspace "medusa"
        When I load 100 rows in the "medusa.test1" table
        When I create the "test2" table in keyspace "medusa"
        When I load 100 rows in the "medusa.test2" table
        When I run a "ccm node1 nodetool -- -Dcom.sun.jndi.rmiURLParsing=legacy flush" command
        Then I have 100 rows in the "medusa.test1" table in ccm cluster "<client encryption>"
        Then I have 100 rows in the "medusa.test2" table in ccm cluster "<client encryption>"
        When I perform a backup in "full" mode of the node named "first_backup" with md5 checks "disabled"
        Then I can see the backup named "first_backup" when I list the backups
        Then I can see the backup status for "first_backup" when I run the status command
        Then I can verify the backup named "first_backup" with md5 checks "disabled" successfully
        When I truncate the "medusa.test1" table in ccm cluster "<client encryption>"
        When I truncate the "medusa.test2" table in ccm cluster "<client encryption>"
        When I restore the backup named "first_backup" for "medusa.test2" table
        Then I have 100 rows in the "medusa.test2" table in ccm cluster "<client encryption>"
        Then I have 0 rows in the "medusa.test1" table in ccm cluster "<client encryption>"

        @local
        Examples: Local storage
        | storage           | client encryption |
        | local      |  with_client_encryption |

        @s3
        Examples: S3 storage
        | storage           | client encryption |
        | s3_us_west_oregon     |  without_client_encryption |

        @gcs
        Examples: Google Cloud Storage
        | storage           | client encryption |
        | google_storage      |  without_client_encryption |

        @azure
        Examples: Azure Blob Storage
        | storage           | client encryption |
        | azure_blobs      | without_client_encryption |
        
        @ibm
        Examples: IBM Cloud Object Storage
        | storage           | client encryption |
        | ibm_storage      | without_client_encryption |

        @minio
        Examples: MinIO storage
        | storage           | client encryption         |
        | minio             | without_client_encryption |

    @13
    Scenario Outline: Perform a backup and a restore, then verify the restore
        Given I have a fresh ccm cluster "<client encryption>" running named "scenario13"
        Given I am using "<storage>" as storage provider in ccm cluster "<client encryption>"
        When I create the "test" table in keyspace "medusa"
        When I load 100 rows in the "medusa.test" table
        When I run a "ccm node1 nodetool -- -Dcom.sun.jndi.rmiURLParsing=legacy flush" command
        When I perform a backup in "full" mode of the node named "first_backup" with md5 checks "disabled"
        Then I can verify the backup named "first_backup" with md5 checks "disabled" successfully
        When I restore the backup named "first_backup"
        Then I can verify the restore verify query "SELECT * FROM medusa.test" returned 100 rows

        @local
        Examples: Local storage
        | storage           | client encryption |
        | local      |  with_client_encryption |

        @s3
        Examples: S3 storage
        | storage           | client encryption |
        | s3_us_west_oregon     |  without_client_encryption |

        @gcs
        Examples: Google Cloud Storage
        | storage           | client encryption |
        | google_storage      |  without_client_encryption |

        @azure
        Examples: Azure Blob Storage
        | storage           | client encryption |
        | azure_blobs      | without_client_encryption |
        
        @ibm
        Examples: IBM Cloud Object Storage
        | storage           | client encryption |
        | ibm_storage      | without_client_encryption |

        @minio
        Examples: MinIO storage
        | storage           | client encryption         |
        | minio             | without_client_encryption |

    @14
    Scenario Outline: Perform a backup & restore of a table with secondary index
        Given I have a fresh ccm cluster "<client encryption>" running named "scenario14"
        Given I am using "<storage>" as storage provider in ccm cluster "<client encryption>"
        When I create the "test" table with secondary index in keyspace "medusa"
        When I load 100 rows in the "medusa.test" table
        When I run a "ccm node1 nodetool -- -Dcom.sun.jndi.rmiURLParsing=legacy flush" command
        When I perform a backup in "differential" mode of the node named "first_backup" with md5 checks "disabled"
        Then I can verify the backup named "first_backup" with md5 checks "disabled" successfully
        Then I can see secondary index files in the "first_backup" files
        When I restore the backup named "first_backup"
        Then I have 100 rows in the "medusa.test" table in ccm cluster "<client encryption>"
        Then I can verify the restore verify query "SELECT * FROM medusa.test WHERE value = '0';" returned 1 rows

        @local
        Examples: Local storage
        | storage           | client encryption |
        | local      |  with_client_encryption |

        @s3
        Examples: S3 storage
        | storage           | client encryption |
        | s3_us_west_oregon     |  without_client_encryption |

        @gcs
        Examples: Google Cloud Storage
        | storage           | client encryption |
        | google_storage      |  without_client_encryption |

        @azure
        Examples: Azure Blob Storage
        | storage           | client encryption |
        | azure_blobs      | without_client_encryption |
        
        @ibm
        Examples: IBM Cloud Object Storage
        | storage           | client encryption |
        | ibm_storage      | without_client_encryption |

        @minio
        Examples: MinIO storage
        | storage           | client encryption         |
        | minio             | without_client_encryption |

    @15
    Scenario Outline: Do a full backup, then a differential one
        Given I have a fresh ccm cluster "<client encryption>" running named "scenario15"
        Given I am using "<storage>" as storage provider in ccm cluster "<client encryption>"
        When I create the "test" table in keyspace "medusa"
        When I load 100 rows in the "medusa.test" table
        When I run a "ccm node1 nodetool -- -Dcom.sun.jndi.rmiURLParsing=legacy flush" command
        When I perform a backup in "full" mode of the node named "first_backup" with md5 checks "disabled"
        When I perform a backup in "differential" mode of the node named "second_backup" with md5 checks "disabled"
        Then I can verify the backup named "first_backup" with md5 checks "disabled" successfully
        Then I can verify the backup named "second_backup" with md5 checks "disabled" successfully

        @local
        Examples: Local storage
        | storage           | client encryption |
        | local      |  with_client_encryption |

        @s3
        Examples: S3 storage
        | storage           | client encryption |
        | s3_us_west_oregon     |  without_client_encryption |

        @gcs
        Examples: Google Cloud Storage
        | storage           | client encryption |
        | google_storage      |  without_client_encryption |

        @azure
        Examples: Azure Blob Storage
        | storage           | client encryption |
        | azure_blobs     | without_client_encryption |
        
        @ibm
        Examples: IBM Cloud Object Storage
        | storage           | client encryption |
        | ibm_storage      | without_client_encryption |

        @minio
        Examples: MinIO storage
        | storage           | client encryption         |
        | minio             | without_client_encryption |

    @16
    Scenario Outline: Perform a differential backup over gRPC , verify its index, then delete it over gRPC with Jolokia
        Given I have a fresh ccm cluster with jolokia "<client encryption>" running named "scenario16"
        Given I am using "<storage>" as storage provider in ccm cluster "<client encryption>" with gRPC server
        Then the gRPC server is up
        When I create the "test" table in keyspace "medusa"
        When I load 100 rows in the "medusa.test" table
        When I run a "ccm node1 nodetool -- -Dcom.sun.jndi.rmiURLParsing=legacy flush" command
        When I perform a backup over gRPC in "differential" mode of the node named "grpc_backup_2"
        Then the backup index exists
        Then I verify over gRPC that the backup "grpc_backup_2" exists and is of type "differential"
        Then I can see the backup index entry for "grpc_backup_2"
        Then I can see the latest backup for "127.0.0.1" being called "grpc_backup_2"
        Then I wait for 10 seconds
        When I perform a backup over gRPC in "differential" mode of the node named "grpc_backup_2_2"
        Then I verify over gRPC that the backup "grpc_backup_2_2" exists and is of type "differential"
        And I verify over gRPC that I can see both backups "grpc_backup_2" and "grpc_backup_2_2"
        Then I can see the backup index entry for "grpc_backup_2_2"
        Then I can see the latest backup for "127.0.0.1" being called "grpc_backup_2_2"
        When I perform a purge over gRPC
        Then 1 backup has been purged
        Then I verify over gRPC that the backup "grpc_backup_2" does not exist
        Then I shutdown the gRPC server

        @local
        Examples: Local storage
        | storage           | client encryption |
        | local      |  with_client_encryption |

    @17
    Scenario Outline: Perform a differential backup over gRPC , verify its index, then delete it over gRPC with failures
        Given I have a fresh ccm cluster with jolokia "<client encryption>" running named "scenario17"
        Given I am using "<storage>" as storage provider in ccm cluster "<client encryption>" with gRPC server
        Then the gRPC server is up
        When I create the "test" table in keyspace "medusa"
        When I load 100 rows in the "medusa.test" table
        When I run a "ccm node1 nodetool -- -Dcom.sun.jndi.rmiURLParsing=legacy flush" command
        When I perform a backup over gRPC in "differential" mode of the node named "grpc_backup_1"
        Then the backup index exists
        Then I verify over gRPC that the backup "grpc_backup_1" exists and is of type "differential"
        And I verify over gRPC that the backup "grpc_backup_1" has the expected placement information
        When I perform a backup over gRPC in "differential" mode of the node named "grpc_backup_1" and it fails
        Then I delete the backup "grpc_backup_1" over gRPC
        Then I delete the backup "grpc_backup_1" over gRPC and it fails
        Then I verify over gRPC that the backup "grpc_backup_1" does not exist
        Then I shutdown the gRPC server

        @local
        Examples: Local storage
        | storage           | client encryption |
        | local      |  with_client_encryption |

        @s3
        Examples: S3 storage
        | storage           | client encryption |
        | s3_us_west_oregon     |  without_client_encryption |

    @18 @skip-cassandra-2
    Scenario Outline: Perform differential backups over gRPC , verify its index, then delete it over gRPC with management API
        Given I have a fresh ccm cluster with mgmt api "<client encryption>" named "scenario18"
        Given I am using "<storage>" as storage provider in ccm cluster "<client encryption>" with mgmt api
        Then the gRPC server is up
        When I create the "test" table in keyspace "medusa"
        When I load 100 rows in the "medusa.test" table
        When I run a "ccm node1 nodetool -- -Dcom.sun.jndi.rmiURLParsing=legacy flush" command
        When I perform a backup over gRPC in "differential" mode of the node named "grpc_backup_2"
        Then the backup index exists
        Then I verify over gRPC that the backup "grpc_backup_2" exists and is of type "differential"
        Then I can see the backup index entry for "grpc_backup_2"
        Then I can see the latest backup for "127.0.0.1" being called "grpc_backup_2"
        Then I wait for 10 seconds
        When I perform a backup over gRPC in "differential" mode of the node named "grpc_backup_2_2"
        Then I verify over gRPC that the backup "grpc_backup_2_2" exists and is of type "differential"
        Then I can see the backup index entry for "grpc_backup_2_2"
        Then I can see the latest backup for "127.0.0.1" being called "grpc_backup_2_2"
        When I perform a purge over gRPC
        Then 1 backup has been purged
        Then I verify over gRPC that the backup "grpc_backup_2" does not exist
        Then I shutdown the gRPC server
        Then I shutdown the mgmt api server

        @local
        Examples: Local storage
        | storage           | client encryption |
        | local      |  with_client_encryption |
    
    @19
    Scenario Outline: Test backup gc grace period with purge
        Given I have a fresh ccm cluster "<client encryption>" running named "scenario19"
        Given I am using "<storage>" as storage provider in ccm cluster "<client encryption>"
        When I create the "test" table in keyspace "medusa"
        When I load 100 rows in the "medusa.test" table
        When I run a "ccm node1 nodetool -- -Dcom.sun.jndi.rmiURLParsing=legacy flush" command
        When I perform a backup in "differential" mode of the node named "first_backup" with md5 checks "disabled"
        When I perform a backup in "differential" mode of the node named "second_backup" with md5 checks "disabled"
        When I load 100 rows in the "medusa.test" table
        When I run a "ccm node1 nodetool -- -Dcom.sun.jndi.rmiURLParsing=legacy flush" command
        When I run a "ccm node1 nodetool -- -Dcom.sun.jndi.rmiURLParsing=legacy compact medusa" command
        When I load 100 rows in the "medusa.test" table
        When I run a "ccm node1 nodetool -- -Dcom.sun.jndi.rmiURLParsing=legacy flush" command
        When I perform a backup in "differential" mode of the node named "third_backup" with md5 checks "disabled"
        When I load 100 rows in the "medusa.test" table
        When I run a "ccm node1 nodetool -- -Dcom.sun.jndi.rmiURLParsing=legacy flush" command
        When I perform a backup in "differential" mode of the node named "fourth_backup" with md5 checks "disabled"
        When I run a "ccm node1 nodetool -- -Dcom.sun.jndi.rmiURLParsing=legacy compact medusa" command
        When I perform a backup in "differential" mode of the node named "fifth_backup" with md5 checks "disabled"
        Then I can see the backup named "first_backup" when I list the backups
        Then I can see the backup named "second_backup" when I list the backups
        Then I can see the backup named "third_backup" when I list the backups
        Then I can see the backup named "fourth_backup" when I list the backups
        Then I can see the backup named "fifth_backup" when I list the backups
        Then I can verify the backup named "fifth_backup" with md5 checks "disabled" successfully
        When I purge the backup history to retain only 2 backups
        Then I cannot see the backup named "first_backup" when I list the backups
        Then I cannot see the backup named "second_backup" when I list the backups
        Then I cannot see the backup named "third_backup" when I list the backups
        Then I can actually see purged backup files for the "test" table in keyspace "medusa"
        Then I can see the backup named "fourth_backup" when I list the backups
        Then I can see the backup named "fifth_backup" when I list the backups
        Then I can verify the backup named "fourth_backup" with md5 checks "disabled" successfully
        Then I can verify the backup named "fifth_backup" with md5 checks "disabled" successfully
        
        @local
        Examples: Local storage
        | storage                    | client encryption |
        | local_backup_gc_grace      |  with_client_encryption |

    @20
    Scenario Outline: Create an incomplete backup, verify it and delete it
        Given I have a fresh ccm cluster "<client encryption>" running named "scenario20"
        Given I am using "<storage>" as storage provider in ccm cluster "<client encryption>"
        When I create the "test" table in keyspace "medusa"
        When I load 100 rows in the "medusa.test" table
        When I perform a backup in "full" mode of the node named "first_backup" with md5 checks "disabled"
        Then I can verify the backup named "first_backup" with md5 checks "enabled" successfully
        And I delete the manifest from the backup named "first_backup"
        Then I can see the backup named "first_backup" when I list the backups
        And the backup named "first_backup" is incomplete
        And verifying backup "first_backup" fails
        When I delete the backup named "first_backup"
        Then I cannot see the backup named "first_backup" when I list the backups

        @local
        Examples: Local storage
        | storage           | client encryption |
        | local      |  with_client_encryption |
    
    @21
    Scenario Outline: Create a corrupt backup, verify it and delete it
        Given I have a fresh ccm cluster "<client encryption>" running named "scenario21"
        Given I am using "<storage>" as storage provider in ccm cluster "<client encryption>"
        When I create the "test" table in keyspace "medusa"
        When I load 100 rows in the "medusa.test" table
        When I perform a backup in "full" mode of the node named "first_backup" with md5 checks "disabled"
        Then I can verify the backup named "first_backup" with md5 checks "enabled" successfully
        And I "delete" a random sstable from "full" backup "first_backup" in the "test" table in keyspace "medusa"
        Then verifying backup "first_backup" fails
        When I delete the backup named "first_backup"
        Then I cannot see the backup named "first_backup" when I list the backups

        @local
        Examples: Local storage
        | storage           | client encryption |
        | local      |  with_client_encryption |

    @22
    Scenario Outline: Delete a backup with the presence of an incomplete backup
        Given I have a fresh ccm cluster "<client encryption>" running named "scenario22"
        Given I am using "<storage>" as storage provider in ccm cluster "<client encryption>"
        When I create the "test" table in keyspace "medusa"
        When I load 100 rows in the "medusa.test" table
        When I run a "ccm node1 nodetool -- -Dcom.sun.jndi.rmiURLParsing=legacy flush" command
        When I perform a backup in "differential" mode of the node named "first_backup" with md5 checks "disabled"
        When I load 100 rows in the "medusa.test" table
        When I run a "ccm node1 nodetool -- -Dcom.sun.jndi.rmiURLParsing=legacy flush" command
        When I run a "ccm node1 nodetool -- -Dcom.sun.jndi.rmiURLParsing=legacy compact medusa" command
        When I perform a backup in "differential" mode of the node named "second_backup" with md5 checks "disabled"
        When I load 100 rows in the "medusa.test" table
        When I run a "ccm node1 nodetool -- -Dcom.sun.jndi.rmiURLParsing=legacy flush" command
        When I run a "ccm node1 nodetool -- -Dcom.sun.jndi.rmiURLParsing=legacy compact medusa" command
        When I perform a backup in "differential" mode of the node named "third_backup" with md5 checks "disabled"
        Then I can see the backup named "first_backup" when I list the backups
        Then I can see the backup named "second_backup" when I list the backups
        And I delete the manifest from the backup named "second_backup" from the storage
        And the backup named "second_backup" is incomplete
        Then I can see the backup named "third_backup" when I list the backups
        When I delete the backup named "first_backup"
        Then I cannot see the backup named "first_backup" when I list the backups
        Then I can see the backup named "second_backup" when I list the backups
        Then I can see the backup named "third_backup" when I list the backups

        @local
        Examples: Local storage
        | storage | client encryption      |
        | local   | with_client_encryption |

        @s3
        Examples: S3 storage
        | storage           | client encryption         |
        | s3_us_west_oregon | without_client_encryption |

        @gcs
        Examples: Google Cloud Storage
        | storage        | client encryption         |
        | google_storage | without_client_encryption |

        @azure
        Examples: Azure Blob Storage
        | storage     | client encryption         |
        | azure_blobs | without_client_encryption |

        @ibm
        Examples: IBM Cloud Object Storage
        | storage     | client encryption         |
        | ibm_storage | without_client_encryption |

        @minio
        Examples: MinIO storage
        | storage | client encryption         |
        | minio   | without_client_encryption |


    @23
    Scenario Outline: Perform a differential async backup over gRPC, verify its index, then delete it over gRPC with Jolokia
        Given I have a fresh ccm cluster with jolokia "<client encryption>" running named "scenario23"
        Given I am using "<storage>" as storage provider in ccm cluster "<client encryption>" with gRPC server
        Then the gRPC server is up
        When I create the "test" table in keyspace "medusa"
        When I load 100 rows in the "medusa.test" table
        When I run a "ccm node1 nodetool -- -Dcom.sun.jndi.rmiURLParsing=legacy flush" command
        When I perform an async backup over gRPC in "differential" mode of the node named "grpc_backup_23"
        Then I can tell a backup "is" in progress
        Then I wait for the async backup "grpc_backup_23" to finish
        Then I can tell a backup "is not" in progress
        Then the backup index exists
        Then I verify over gRPC that the backup "grpc_backup_23" exists and is of type "differential"
        Then I can see the backup index entry for "grpc_backup_23"
        Then I can see the latest backup for "127.0.0.1" being called "grpc_backup_23"
        Then I verify over gRPC that the backup "grpc_backup_23" has expected status SUCCESS
        Then I delete the backup "grpc_backup_23" over gRPC
        Then I verify over gRPC that the backup "grpc_backup_23" does not exist
        Then I verify that backup manager has removed the backup "grpc_backup_23"
        Then I shutdown the gRPC server

        @local
        Examples: Local storage
        | storage                    | client encryption |
        | local_backup_gc_grace      |  with_client_encryption |
    
    @24
    Scenario Outline: Run purge with nodes sharing the same prefix as fqdn
        Given I am using "<storage>" as storage provider in ccm cluster "<client encryption>"
        When node "127.0.0.10" fakes a complete backup named "backup1" on "2019-04-15 12:12:00"
        And node "127.0.0.101" fakes a complete backup named "backup1" on "2019-04-15 12:14:00"
        And node "127.0.0.102" fakes a complete backup named "backup1" on "2019-04-15 12:14:00"
        And node "127.0.0.10" fakes a complete backup named "backup2" on "2019-04-01 12:14:00"
        And node "127.0.0.101" fakes a complete backup named "backup2" on "2019-04-01 12:14:00"
        And node "127.0.0.102" fakes a complete backup named "backup2" on "2019-04-01 12:14:00"
        Then listing backups for node "127.0.0.10" returns 2 backups
        

        @local
        Examples: Local storage
        | storage           | client encryption |
        | local      |  with_client_encryption |
    

    @25
    Scenario Outline: Perform an differential backup, verify it, modify Statistics.db file, verify it
        Given I have a fresh ccm cluster "<client encryption>" running named "scenario25"
        Given I am using "<storage>" as storage provider in ccm cluster "<client encryption>"
        When I create the "test" table in keyspace "medusa"
        When I load 100 rows in the "medusa.test" table
        When I run a "ccm node1 nodetool -- -Dcom.sun.jndi.rmiURLParsing=legacy flush" command
        When I perform a backup in "differential" mode of the node named "first_backup" with md5 checks "enabled"
        Then I can see the backup named "first_backup" when I list the backups
        Then I can verify the backup named "first_backup" with md5 checks "enabled" successfully
        Then I modify Statistics.db file in the backup in the "test" table in keyspace "medusa"
        Then I can verify the backup named "first_backup" with md5 checks "enabled" successfully
        

        @local
        Examples: Local storage
        | storage           | client encryption |
        | local      |  with_client_encryption |  

    # TODO: some steps weren't implemented. Disabling the test until all steps are.
    #@26
    #Scenario Outline: Test purge of decommissioned nodes
    #Given I have a fresh ccm cluster "<client encryption>" running named "scenario26"
    #    Given I am using "<storage>" as storage provider in ccm cluster "<client encryption>"
    #    When node "127.0.0.2" fakes a complete backup named "backup1" on "2019-04-15 12:12:00"
    #    Then I can see the backup named "backup1" when I list the backups
    #    When I create the "test" table in keyspace "medusa"
    #    When I perform a backup in "differential" mode of the node named "backup2" with md5 checks "disabled"
    #    Then checking the list of decommissioned nodes returns "127.0.0.2"
    #    When I run a purge on decommissioned nodes
    #    Then I cannot see the backup named "backup1" when I list the backups
    #    Then I can see the backup named "backup2" when I list the backups
    #
    #    
    #
    #    @local
    #    Examples: Local storage
    #    | storage           | client encryption |
    #    | local      |  with_client_encryption |

    @27
    Scenario Outline: Write a lot of files to storage, then try to list them
    Given I am using "<storage>" as storage provider in ccm cluster "<client encryption>"
    When I write "1042" files to storage
    Then I can list all "1042" files in the storage
    Then I clean up the files

    @local
    Examples: Local storage
    | storage    | client encryption |
    | local      | without_client_encryption |

    @minio
    Examples: MinIO storage
    | storage | client encryption         |
    | minio   | without_client_encryption |

    # skipping s3 because we don't have good enough parallelization yet and this scenario takes too long

    @gcs
    Examples: Google Cloud Storage
    | storage        | client encryption         |
    | google_storage | without_client_encryption |

    @azure
    Examples: Azure Blob Storage
    | storage     | client encryption         |
    | azure_blobs | without_client_encryption |

    @28
    Scenario Outline: Make a 2 node cluster, backup 1 node, check getting status works
        Given I have a fresh "2" node ccm cluster with jolokia "<client encryption>" running named "scenario28"
        Given I am using "<storage>" as storage provider in ccm cluster "<client encryption>" with gRPC server
        Then the gRPC server is up
        When I create the "test" table in keyspace "medusa"
        When I load 100 rows in the "medusa.test" table
        When I run a "ccm node1 nodetool -- -Dcom.sun.jndi.rmiURLParsing=legacy flush" command
        When I run a "ccm node2 nodetool -- -Dcom.sun.jndi.rmiURLParsing=legacy flush" command
        When I perform an async backup over gRPC in "differential" mode of the node named "grpc_backup_28"
        Then I wait for the async backup "grpc_backup_28" to finish
        Then the backup index exists
        Then I verify over gRPC that the backup "grpc_backup_28" exists and is of type "differential"
        # The backup status is not actually a SUCCESS because the node2 has not been backed up.
        # The gRPC server is not meant to understand other nodes.
        # We just have to check so that we hit the case where a missing node was causing an exception
        Then I verify over gRPC that the backup "grpc_backup_28" has expected status SUCCESS
        # now we've checked that, so we can clean up and end
        Then I delete the backup "grpc_backup_28" over gRPC
        Then I verify over gRPC that the backup "grpc_backup_28" does not exist
        Then I verify that backup manager has removed the backup "grpc_backup_28"
        Then I shutdown the gRPC server

        @local
        Examples: Local storage
        | storage                    | client encryption |
        | local_backup_gc_grace      | without_client_encryption |

    @29
    Scenario Outline: Backup and restore a DSE cluster with search enabled
        Given I have a fresh DSE cluster version "6.8.38" with "<client encryption>" running named "scenario29"
        Given I am using "<storage>" as storage provider in ccm cluster "<client encryption>" with gRPC server
        Then the gRPC server is up
        When I create the "test" table in keyspace "medusa"
        And I create a search index on the "test" table in keyspace "medusa"
        And I load 100 rows in the "medusa.test" table
        When I run a DSE "nodetool flush" command
        Then I can make a search query against the "medusa"."test" table
        When I perform an async backup over gRPC in "differential" mode of the node named "backup29-1"
        Then I wait for the async backup "backup29-1" to finish
        Then the backup index exists
        Then I can see the backup named "backup29-1" when I list the backups
        And the backup "backup29-1" has server_type "dse" in its metadata
        Then I verify over gRPC that the backup "backup29-1" exists and is of type "differential"
        Then I verify over gRPC that the backup "backup29-1" has expected status SUCCESS
        When I perform an async backup over gRPC in "differential" mode of the node named "backup29-2"
        Then I wait for the async backup "backup29-2" to finish
        Then the backup index exists
        Then I can see the backup named "backup29-2" when I list the backups
        And the backup "backup29-2" has server_type "dse" in its metadata
        Then I verify over gRPC that the backup "backup29-2" exists and is of type "differential"
        Then I verify over gRPC that the backup "backup29-2" has expected status SUCCESS
        When I restore the backup named "backup29-2"
        And I wait for the DSE Search indexes to be rebuilt
        Then I have 100 rows in the "medusa.test" table in ccm cluster "<client encryption>"
        Then I can make a search query against the "medusa"."test" table
        Then I stop the DSE cluster
        And I delete the DSE cluster

    @local
    Examples: Local storage
    | storage    | client encryption |
    | local      | without_client_encryption |

    @s3
    Examples: S3 storage
    | storage           | client encryption         |
    | s3_us_west_oregon | without_client_encryption |

    @30
    Scenario Outline: Create an differential backup, corrupt it, then fix by doing another backup, and verify it
        Given I have a fresh ccm cluster "<client encryption>" running named "scenario30"
        Given I am using "<storage>" as storage provider in ccm cluster "<client encryption>"
        When I create the "test" table with secondary index in keyspace "medusa"
        When I load 100 rows in the "medusa.test" table
        When I perform a backup in "differential" mode of the node named "first_backup" with md5 checks "disabled"
        Then I can verify the backup named "first_backup" with md5 checks "enabled" successfully
        And I "delete" a random sstable from "differential" backup "first_backup" in the "test" table in keyspace "medusa"
        Then verifying backup "first_backup" fails
        When I perform a backup in "differential" mode of the node named "second_backup" with md5 checks "disabled"
        Then I can verify the backup named "first_backup" with md5 checks "enabled" successfully
        Then I can verify the backup named "second_backup" with md5 checks "enabled" successfully
        And I "truncate" a random sstable from "differential" backup "second_backup" in the "test" table in keyspace "medusa"
        Then verifying backup "second_backup" fails
        When I perform a backup in "differential" mode of the node named "fourth_backup" with md5 checks "enabled"
        Then some files from the previous backup were not reuploaded
        Then some files from the previous backup "were" replaced
        Then I can verify the backup named "second_backup" with md5 checks "enabled" successfully
        Then I can verify the backup named "fourth_backup" with md5 checks "enabled" successfully
        When I delete the backup named "first_backup"
        Then I cannot see the backup named "first_backup" when I list the backups

    @local
    Examples: Local storage
    | storage    | client encryption |
    | local      |  with_client_encryption |

    @31
    Scenario Outline: Perform a backup, then forget about it, then get its status over gRPC
        Given I have a fresh ccm cluster with jolokia "<client encryption>" running named "scenario31"
        Given I am using "<storage>" as storage provider in ccm cluster "<client encryption>" with gRPC server
        Then the gRPC server is up
        When I create the "test" table in keyspace "medusa"
        When I load 100 rows in the "medusa.test" table
        When I run a "ccm node1 nodetool -- -Dcom.sun.jndi.rmiURLParsing=legacy flush" command
        When I perform a backup in "differential" mode of the node named "first_backup" with md5 checks "enabled"
        Then the backup index exists
        When I forget about all backups
        Then I verify over gRPC that the backup "first_backup" has status "SUCCESS"
        Then I shutdown the gRPC server

    @local
    Examples: Local storage
    | storage    | client encryption |
    | local      |  with_client_encryption |
