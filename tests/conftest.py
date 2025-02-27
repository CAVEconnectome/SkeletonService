import logging

import pytest

from skeletonservice import create_app
from skeletonservice.datasets.service_skvn1 import SkeletonService_skvn1
from skeletonservice.datasets.service_skvn2 import SkeletonService_skvn2
from skeletonservice.datasets.service_skvn3 import SkeletonService_skvn3
from skeletonservice.datasets.service_skvn4 import SkeletonService_skvn4

from cloudfiles import CloudFiles
from messagingclient import MessagingClient

from caveclient.tools.testing import (
    CAVEclientMock,
    default_info,
    get_server_information,
)

test_logger = logging.getLogger(__name__)

test_config = {
    "ENVIRONMENT": "asdf",
    "SKELETON_VERSION_ENGINES": {
        1: SkeletonService_skvn1,
        2: SkeletonService_skvn2,
        3: SkeletonService_skvn3,
        4: SkeletonService_skvn4,
    }

}

datastack_dict = get_server_information()

test_info = default_info(datastack_dict["local_server"])

class CloudVolumeMock:
    class CloudVolumeMockMetaMock:
        def __init__(self):
            self.n_layers = 1
        
        def decode_layer_id(self, root_id):
            return 1
    
    def __init__(self):
        self.meta = CloudVolumeMock.CloudVolumeMockMetaMock()

# From MaterializationEngine:conftest.py
# Setup Flask apps
@pytest.fixture(scope="session")
def test_app():
    flask_app = create_app(test_config=test_config)
    test_logger.info("Starting test flask app...")

    # Create a test client using the Flask application configured for testing
    with flask_app.test_client() as testing_client:
        # Establish an application context
        with flask_app.app_context():
            yield testing_client  #

@pytest.fixture()
def caveclient_mock():
    return CAVEclientMock(
        chunkedgraph=True,
        materialization=True,
        json_service=True,
        skeleton_service=True,
        l2cache=True,
        global_server=datastack_dict["global_server"],
    )

@pytest.fixture()
def cloudfiles_mock():
    return CloudFiles("gs://test_bucket")

@pytest.fixture()
def cloudvolume_mock():
    return CloudVolumeMock()

@pytest.fixture()
def messagingclient_mock():
    return MessagingClient()
