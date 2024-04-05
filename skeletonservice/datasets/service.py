from typing import List
import pandas as pd

from skeletonservice.datasets.models import (
    Skeleton,
)
from skeletonservice.datasets.schemas import (
    SkeletonSchema,
)

class SkeletonService:
    @staticmethod
    def get_all() -> List[Skeleton]:
        return [{"name": "Skeleton #1"}]  # Skeleton.query.all()

    @staticmethod
    def get_skeleton_by_name(skeleton_name: str) -> Skeleton:
        # return Skeleton.query.filter_by(name=skeleton_name).first_or_404()
        
        with open(f"mock_skeleton_{skeleton_name}.csv") as f:
            sk_df = pd.read_csv(f)
        sk = Skeleton()
        sk.display_name = skeleton_name
        sk.name = skeleton_name
        return sk
