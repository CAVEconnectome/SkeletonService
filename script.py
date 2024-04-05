from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker

from skeletonservice.database import Base
from skeletonservice.datasets.models import DataSet
engine = create_engine("postgres://postgres:synapsedb@localhost:5432/infoservice")
session = sessionmaker(bind=engine)
db_session = scoped_session(session)

# Adds Query Property to Models - enables `User.query.query_method()`
Base.query = db_session.query_property()

# Create Tables
Base.metadata.create_all(bind=engine)
dataset = DataSet(name="pinky100",
                  flat_segmentation_source="gs://neuroglancer/pinky100_v0/seg/lost_no-random/bbox1_0",
                  annotation_engine_endpoint="https://www.dynamicannotationengine.com/",
                  image_source="gs://neuroglancer/pinky100_v0/son_of_alignment_v15_rechunked",
                  pychunkgraph_segmentation_source="gs://neuroglancer/nkem/pinky100_v0/ws/lost_no-random/bbox1_0",
                  pychunkgraph_endpoint="http://35.237.202.194")

db_session.add(dataset)

dataset = DataSet(name="basil",
                  flat_segmentation_source="gs://neuroglancer/basil_v0/basil_full/seg-aug",
                  annotation_engine_endpoint="https://www.dynamicannotationengine.com/",
                  image_source="gs://neuroglancer/basil_v0/son_of_alignment/v3.04_cracks_only_normalized_rechunked",
                  pychunkgraph_segmentation_source="",
                  pychunkgraph_endpoint="http://35.237.202.194")
db_session.add(dataset)

dataset = DataSet(name="pinky100dev",
                  flat_segmentation_source="https://storage.googleapis.com/neuroglancer/pinky100_v0/seg/lost_no-random/bbox1_0",
                  annotation_engine_endpoint="https://www.dynamicannotationengine.com/",
                  image_source="https://storage.googleapis.com/neuroglancer/pinky100_v0/son_of_alignment_v15_rechunked",
                  pychunkgraph_segmentation_source="https://storage.googleapis.com/neuroglancer/nkem/pinky100_v0/ws/lost_no-random/bbox1_0",
                  pychunkgraph_endpoint="http://35.237.202.194")
db_session.add(dataset)

db_session.commit()
print(DataSet.query.all())
