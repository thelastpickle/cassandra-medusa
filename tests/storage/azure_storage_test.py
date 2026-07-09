# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import asyncio
import os
import tempfile
import unittest

from azure.core.credentials import AzureNamedKeyCredential
from azure.identity import DefaultAzureCredential

from medusa.storage.azure_storage import AzureStorage
from tests.storage.abstract_storage_test import AttributeDict


class AzureStorageTest(unittest.TestCase):

    credentials_file_content = """
    {
      "storage_account": "medusa-unit-test",
      "key": "randomString=="
    }
    """

    def test_make_connection_url(self):
        with tempfile.NamedTemporaryFile() as credentials_file:
            credentials_file.write(self.credentials_file_content.encode())
            credentials_file.flush()
            config = AttributeDict({
                'region': 'region-from-config',
                'storage_provider': 'azure_blobs',
                'key_file': credentials_file.name,
                'bucket_name': 'bucket-from-config',
                'concurrent_transfers': '1',
                'host': None,
                'port': None,
                'read_timeout': 60,
            })
            azure_storage = AzureStorage(config)
            self.assertIsInstance(azure_storage.credentials, AzureNamedKeyCredential)
            self.assertEqual(
                'https://medusa-unit-test.blob.core.windows.net/',
                azure_storage.azure_blob_service_url
            )

    def test_make_connection_url_with_custom_host(self):
        with tempfile.NamedTemporaryFile() as credentials_file:
            credentials_file.write(self.credentials_file_content.encode())
            credentials_file.flush()
            config = AttributeDict({
                'region': 'region-from-config',
                'storage_provider': 'azure_blobs',
                'key_file': credentials_file.name,
                'bucket_name': 'bucket-from-config',
                'concurrent_transfers': '1',
                'host': 'custom.host.net',
                'port': None,
                'read_timeout': 60,
            })
            azure_storage = AzureStorage(config)
            self.assertIsInstance(azure_storage.credentials, AzureNamedKeyCredential)
            self.assertEqual(
                'https://medusa-unit-test.blob.core.custom.host.net/',
                azure_storage.azure_blob_service_url
            )

    def test_make_connection_url_with_custom_host_port(self):
        with tempfile.NamedTemporaryFile() as credentials_file:
            credentials_file.write(self.credentials_file_content.encode())
            credentials_file.flush()
            config = AttributeDict({
                'region': 'region-from-config',
                'storage_provider': 'azure_blobs',
                'key_file': credentials_file.name,
                'bucket_name': 'bucket-from-config',
                'concurrent_transfers': '1',
                'host': 'custom.host.net',
                'port': 123,
                'read_timeout': 60,
            })
            azure_storage = AzureStorage(config)
            self.assertIsInstance(azure_storage.credentials, AzureNamedKeyCredential)
            self.assertEqual(
                'https://medusa-unit-test.blob.core.custom.host.net:123/',
                azure_storage.azure_blob_service_url
            )

    def test_use_default_azure_credentials(self):
        config = AttributeDict({
            'region': 'region-from-config',
            'storage_provider': 'azure_blobs',
            'bucket_name': 'bucket-from-config',
            'concurrent_transfers': '1',
            'host': None,
            'port': None,
            'read_timeout': 60,
            'key_file': None,
        })
        os.environ['AZURE_STORAGE_ACCOUNT'] = 'testAccount'
        azure_storage = AzureStorage(config)
        self.assertIsInstance(azure_storage.credentials, DefaultAzureCredential)
        # we need the account name for making the connection url
        self.assertEqual(
            'https://testAccount.blob.core.windows.net/',
            azure_storage.azure_blob_service_url
        )

    def test_list_blobs_skips_directory_placeholders(self):
        with tempfile.NamedTemporaryFile() as credentials_file:
            credentials_file.write(self.credentials_file_content.encode())
            credentials_file.flush()
            config = AttributeDict({
                'region': 'region-from-config',
                'storage_provider': 'azure_blobs',
                'key_file': credentials_file.name,
                'bucket_name': 'bucket-from-config',
                'concurrent_transfers': '1',
                'host': None,
                'port': None,
                'read_timeout': 60,
            })
            azure_storage = AzureStorage(config)
            azure_storage.connect()

            async def fake_list_blobs(name_starts_with=None, include=None, **kwargs):
                for props in (
                    # ADLS Gen2 / hierarchical-namespace directory marker: must be skipped
                    AttributeDict({
                        'name': 'some/dir/',
                        'size': 0,
                        'etag': 'etag1',
                        'last_modified': None,
                        'blob_tier': None,
                        'metadata': {'hdi_isfolder': 'true'},
                    }),
                    # legitimately empty SSTable component (e.g. BTI Rows.db): must be kept
                    AttributeDict({
                        'name': 'some/dir/da-1-bti-Rows.db',
                        'size': 0,
                        'etag': 'etag2',
                        'last_modified': None,
                        'blob_tier': None,
                        'metadata': {},
                    }),
                ):
                    yield props

            azure_storage.azure_container_client.list_blobs = fake_list_blobs

            blobs = asyncio.run(azure_storage._list_blobs())

            self.assertEqual(1, len(blobs))
            self.assertEqual('some/dir/da-1-bti-Rows.db', blobs[0].name)
