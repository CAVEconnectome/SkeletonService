import logging
from unittest.mock import patch

import pytest

from skeletonservice import create_app
from skeletonservice.datasets.service_skvn1 import SkeletonService_skvn1
from skeletonservice.datasets.service_skvn2 import SkeletonService_skvn2
from skeletonservice.datasets.service_skvn3 import SkeletonService_skvn3
from skeletonservice.datasets.service_skvn4 import SkeletonService_skvn4

from cloudfiles import CloudFiles
from messagingclient import MessagingClientPublisher

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

@pytest.fixture(autouse=True)
def no_real_pubsub():
    """Keep PublisherClient construction offline.

    MessagingClientPublisher.__init__ builds a real pubsub_v1.PublisherClient, so every test that
    constructs one -- directly, or indirectly through the bulk generate paths -- needs Application
    Default Credentials. That made the suite pass on a workstation with gcloud configured and fail
    in the container, which is backwards for unit tests.
    """
    with patch("google.cloud.pubsub_v1.PublisherClient"):
        yield


@pytest.fixture(autouse=True)
def undo_started_patches():
    """Undo any `patch(...).start()` a test left running.

    The suite starts patches without stopping them, so a class or method one test replaces
    stays replaced for every test that follows. That makes results order-dependent: the same
    test passes alone and fails in a full run, depending on what ran before it.

    patch.stopall() only reverts patches started via `start()`, so it is a no-op for tests
    that already use `with patch(...)` or a decorator.
    """
    yield
    patch.stopall()


@pytest.fixture(autouse=True)
def clear_cave_client_cache():
    """Empty the module-level CAVEclient cache around every test.

    SkeletonService._get_cave_client memoises clients for CAVE_CLIENT_CACHE_TTL_S seconds, which
    is process-wide state. Without this, a client built while `caveclient.CAVEclient` was patched
    survives into later tests -- the patch is reverted by undo_started_patches above, but the mock
    instance it produced is still sitting in the cache, so the next test silently receives a stale
    mock instead of constructing its own. That made three async tests pass alone and fail in a
    full run.

    Cleared before as well as after so the ordering cannot matter.
    """
    from skeletonservice.datasets import service as _svc

    _svc._cave_client_cache.clear()
    yield
    _svc._cave_client_cache.clear()


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
    return MessagingClientPublisher(100)
