<!--
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
-->

# GCP Permissions Needed For Medusa To Operate

This document describes what GCP permissions are needed at various stages of setting up and using Medusa.

When dealing with GCP permissions, it is a good practice to group these into roles.
At the very least, allowing a service account to manipulate objects in GCS happens via a IAM role, not via an account.
That is why we recommend establishing three roles:
  - `setup` role - used to create buckets and service accounts for the backup and restore parts.
  - `backup` role - used to upload objects.
  - `restore` role - used for restores. This is a version of the `backup` role but with read-only access.


## Permissions for setting up Medusa

The permissions needed to set Medusa up are:
  - `storage.buckets.create` - to create a bucket where Medusa uploads.
  - `iam.serviceAccounts.create` - to create the service account Medusa will run under.
  - `iam.serviceAccountKeys.create` - to create a key for this account.
  - `storage.buckets.getIamPolicy` - to get the IAM policy of the newly created bucket.
  - `storage.buckets.setIamPolicy` - to set the IAM policy, so the newly created account can be used.


## Permissions for doing backups

Doing backups with Medusa needs these permissions:
  - `storage.buckets.get`
  - `storage.buckets.getIamPolicy`
  - `storage.objects.create`
  - `storage.objects.delete`
  - `storage.objects.get`
  - `storage.objects.getIamPolicy`
  - `storage.objects.list`


## Permissions for doing restores

These are the permissions needed to restore data using Medusa.
  - `storage.buckets.get`
  - `storage.buckets.getIamPolicy`
  - `storage.objects.get`
  - `storage.objects.getIamPolicy`
  - `storage.objects.list`
