# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import tempfile
import unittest

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
            })
            azure_storage = AzureStorage(config)
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
            })
            azure_storage = AzureStorage(config)
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
            })
            azure_storage = AzureStorage(config)
            self.assertEqual(
                'https://medusa-unit-test.blob.core.custom.host.net:123/',
                azure_storage.azure_blob_service_url
            )

    def test_calculate_block_size(self):
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
            })
            azure_storage = AzureStorage(config)
            # small file does not lead to a non-default block size
            self.assertEqual(
                4 * 1024 * 1024,
                azure_storage.calculate_block_size(123)
            )
            # a reasonably big file still uses the default chunk
            self.assertEqual(
                4194304,
                azure_storage.calculate_block_size(20 * 1024 * 1024 * 1024)  # 20 GB
            )
            # azure can have, at most, 50000 blocks, so 50k * 4 = 200 GB
            self.assertEqual(
                4294967,
                azure_storage.calculate_block_size(200 * 1024 * 1024 * 1024 + 1)    # 200 GB + 1B
            )
            self.assertEqual(
                5733781,
                azure_storage.calculate_block_size(267 * 1024 * 1024 * 1024)    # 267 GB
            )
            # the biggest block we can have is 100 MB and the block count is 50k, which leads up to 5 TB file
            self.assertEqual(
                104857600,
                azure_storage.calculate_block_size(100 * 1024 * 1024 * 50 * 1000)
            )
            self.assertEqual(
                -1,
                azure_storage.calculate_block_size(100 * 1024 * 1024 * 50 * 1000 + 1)    # 200 GB + 1B
            )
