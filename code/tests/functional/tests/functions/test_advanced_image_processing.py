import json
import os
import sys
from unittest.mock import ANY

from azure.functions import QueueMessage
import pytest
from backend.batch.utilities.helpers.config.config_helper import (
    CONFIG_CONTAINER_NAME,
    CONFIG_FILE_NAME,
)
from pytest_httpserver import HTTPServer
from tests.functional.app_config import AppConfig
from tests.functional.request_matching import RequestMatcher, verify_request_made

sys.path.append(
    os.path.join(os.path.dirname(sys.path[0]), "..", "..", "backend", "batch")
)

from backend.batch.batch_push_results import batch_push_results  # noqa: E402

pytestmark = pytest.mark.functional

FILE_NAME = "image.jpg"


@pytest.fixture
def message(app_config: AppConfig):
    return QueueMessage(
        body=json.dumps(
            {
                "topic": "topic",
                "subject": f"/blobServices/default/{app_config.get('AZURE_BLOB_CONTAINER_NAME')}/documents/blobs/{FILE_NAME}",
                "eventType": "Microsoft.Storage.BlobCreated",
                "id": "id",
                "data": {
                    "api": "PutBlob",
                    "clientRequestId": "46093109-6e51-437f-aa0e-e6912a80a010",
                    "requestId": "5de84904-c01e-006b-47bb-a28f94000000",
                    "eTag": "0x8DC70D2C41ED398",
                    "contentType": "image/jpeg",
                    "contentLength": 115310,
                    "blobType": "BlockBlob",
                    "url": f"https://{app_config.get('AZURE_BLOB_ACCOUNT_NAME')}.blob.core.windows.net/documents/{FILE_NAME}",
                    "sequencer": "00000000000000000000000000005E450000000000001f49",
                    "storageDiagnostics": {
                        "batchId": "952bdc2e-6006-0000-00bb-a20860000000"
                    },
                },
                "dataVersion": "",
                "metadataVersion": "1",
                "eventTime": "2024-05-10T09:22:51.5565464Z",
            }
        )
    )


@pytest.fixture(autouse=True)
def setup_blob_metadata_mocking(httpserver: HTTPServer, app_config: AppConfig):
    httpserver.expect_request(
        f"/{app_config.get('AZURE_BLOB_CONTAINER_NAME')}/{FILE_NAME}",
        method="HEAD",
    ).respond_with_data()

    httpserver.expect_request(
        f"/{app_config.get('AZURE_BLOB_CONTAINER_NAME')}/{FILE_NAME}",
        method="PUT",
    ).respond_with_data()


def test_config_file_is_retrieved_from_storage(
    message: QueueMessage, httpserver: HTTPServer, app_config: AppConfig
):
    # when
    batch_push_results.build().get_user_function()(message)

    # then
    verify_request_made(
        mock_httpserver=httpserver,
        request_matcher=RequestMatcher(
            path=f"/{CONFIG_CONTAINER_NAME}/{CONFIG_FILE_NAME}",
            method="GET",
            headers={
                "Authorization": ANY,
            },
            times=1,
        ),
    )


def test_metadata_is_updated_after_processing(
    message: QueueMessage, httpserver: HTTPServer, app_config: AppConfig
):
    # when
    batch_push_results.build().get_user_function()(message)

    # then
    verify_request_made(
        mock_httpserver=httpserver,
        request_matcher=RequestMatcher(
            path=f"/{app_config.get('AZURE_BLOB_CONTAINER_NAME')}/{FILE_NAME}",
            method="PUT",
            headers={
                "Authorization": ANY,
                # Note: We cannot assert on this header, as the mock server
                # drops headers containing underscores, although Azure Storage
                # accepts it
                # "x-ms-meta-embeddings_added": "true"
            },
            query_string="comp=metadata",
            times=1,
        ),
    )