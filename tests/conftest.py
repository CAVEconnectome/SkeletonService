import logging

import pytest

from skeletonservice import create_app
from skeletonservice.datasets.service_skvn1 import SkeletonService_skvn1
from skeletonservice.datasets.service_skvn2 import SkeletonService_skvn2
from skeletonservice.datasets.service_skvn3 import SkeletonService_skvn3
from skeletonservice.datasets.service_skvn4 import SkeletonService_skvn4

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

# From MaterializationEngine:conftest.py
# Setup Flask apps
@pytest.fixture(scope="session")
def test_app():
    flask_app = create_app(test_config=test_config)
    test_logger.info(f"Starting test flask app...")

    # Create a test client using the Flask application configured for testing
    with flask_app.test_client() as testing_client:
        # Establish an application context
        with flask_app.app_context():
            yield testing_client  #
