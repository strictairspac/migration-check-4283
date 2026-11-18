from sqlalchemy import Column,Integer,String,DateTime,func,ForeignKey
from src.config.database import Base
class Capacity(Base):
 __tablename__='capacity'
 id=Column(Integer,primary_key=True)
 row_limit=Column(Integer,nullable=False,default=1000)
 used=Column(Integer,nullable=False,default=0)
 reserved=Column(Integer,nullable=False,default=0)
class Reservation(Base):
 __tablename__='reservations'
 id=Column(Integer,primary_key=True)
 run_id=Column(Integer,ForeignKey('runs.id'),unique=True,nullable=False)
 rows=Column(Integer,nullable=False)
 status=Column(String(20),nullable=False,default='active')
 reason=Column(String(300),nullable=True)
 created_at=Column(DateTime,server_default=func.now())
class CapacityEvent(Base):
 __tablename__='capacity_events'
 id=Column(Integer,primary_key=True)
 run_id=Column(Integer,nullable=True)
 action=Column(String(20),nullable=False)
 detail=Column(String(300),nullable=False,default='')
 created_at=Column(DateTime,server_default=func.now())
