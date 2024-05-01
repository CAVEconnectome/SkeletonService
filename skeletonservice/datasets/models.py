# from sqlalchemy import Column, Integer, String, Float, ForeignKey, ARRAY
# from sqlalchemy.orm import relationship
# from sqlalchemy.sql.sqltypes import BigInteger
# from skeletonservice.database import Base


class NamedModel(object):
    # id = Column(Integer, primary_key=True)
    # name = Column(String(100), nullable=False, unique=True)

    def __repr__(self):
        return "{}({})".format(self.name, self.id)

    def __getitem__(self, field):
        return self.__dict__[field]


class Skeleton(NamedModel):#, Base):
    __tablename__ = "skeleton"
    # display_name = Column(String(100), nullable=True)
