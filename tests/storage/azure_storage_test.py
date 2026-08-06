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

from unittest.mock import AsyncMock, MagicMock, patch

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

    def _make_config(self, credentials_file_name, extra=None):
        cfg = {
            'region': 'us-east-1',
            'storage_provider': 'azure_blobs',
            'key_file': credentials_file_name,
            'bucket_name': 'test-bucket',
            'concurrent_transfers': '1',
            'host': None,
            'port': None,
            'read_timeout': 60,
            'storage_class': None,
        }
        if extra:
            cfg.update(extra)
        return AttributeDict(cfg)

    def test_connect_uses_multipart_chunksize(self):
        with tempfile.NamedTemporaryFile() as credentials_file:
            credentials_file.write(self.credentials_file_content.encode())
            credentials_file.flush()
            config = self._make_config(credentials_file.name, {'multipart_chunksize': '8MB'})
            storage = AzureStorage(config)

            with patch('medusa.storage.azure_storage.BlobServiceClient') as mock_bsc:
                mock_bsc.return_value.get_container_client.return_value = MagicMock()
                storage.connect()

            mock_bsc.assert_called_once()
            _, kwargs = mock_bsc.call_args
            self.assertEqual(8 * 1024 * 1024, kwargs['max_block_size'])
            self.assertEqual(8 * 1024 * 1024, kwargs['max_chunk_get_size'])

    def test_connect_uses_default_chunksize_when_not_configured(self):
        with tempfile.NamedTemporaryFile() as credentials_file:
            credentials_file.write(self.credentials_file_content.encode())
            credentials_file.flush()
            config = self._make_config(credentials_file.name)  # no multipart_chunksize
            storage = AzureStorage(config)

            with patch('medusa.storage.azure_storage.BlobServiceClient') as mock_bsc:
                mock_bsc.return_value.get_container_client.return_value = MagicMock()
                storage.connect()

            _, kwargs = mock_bsc.call_args
            self.assertEqual(4 * 1024 * 1024, kwargs['max_block_size'])
            self.assertEqual(4 * 1024 * 1024, kwargs['max_chunk_get_size'])

    def test_upload_blob_passes_chunk_size_to_file_chunks(self):
        with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
            tmp_file.write(b'hello medusa')
            tmp_file_name = tmp_file.name

        with tempfile.NamedTemporaryFile() as credentials_file:
            credentials_file.write(self.credentials_file_content.encode())
            credentials_file.flush()
            config = self._make_config(credentials_file.name, {'multipart_chunksize': '8MB'})
            storage = AzureStorage(config)

            # Fake blob properties returned after upload
            mock_blob_props = MagicMock()
            mock_blob_props.name = 'test-object'
            mock_blob_props.size = 12
            mock_blob_props.etag = '"abc123"'
            mock_blob_props.last_modified = None
            mock_blob_props.blob_tier = None
            mock_blob_props.get.side_effect = lambda key, default=None: default

            mock_blob_client = AsyncMock()
            mock_blob_client.get_blob_properties = AsyncMock(return_value=mock_blob_props)

            mock_container = AsyncMock()
            mock_container.upload_blob = AsyncMock(return_value=mock_blob_client)
            storage.azure_container_client = mock_container

            with patch.object(AzureStorage, '_file_chunks') as mock_file_chunks:
                # _file_chunks is an async generator; make it return a minimal one
                async def _fake_chunks(*args, **kwargs):
                    yield b'hello medusa'
                mock_file_chunks.side_effect = _fake_chunks

                asyncio.run(storage._upload_blob(tmp_file_name, 'test-object'))

            mock_file_chunks.assert_called_once_with(
                tmp_file_name, chunk_size=8 * 1024 * 1024
            )
