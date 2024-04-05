import skeletonservice.datasets.models as models
from flask_marshmallow import Marshmallow
import marshmallow

ma = Marshmallow()



class SkeletonSchema(ma.SQLAlchemyAutoSchema):
    class Meta:
        model = models.Skeleton

    skeleton = ma.HyperlinkRelated(
        "api.Skeletonservice_resource"
    )