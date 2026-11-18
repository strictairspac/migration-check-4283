from datetime import datetime,timedelta,timezone
from fastapi import HTTPException
from sqlalchemy import update
from sqlalchemy.exc import IntegrityError
from src.config.database import Session
from src.config.settings import TARGET_ROW_LIMIT,EXECUTOR_STALE_SECONDS
from src.models.capacity import Capacity,Reservation,CapacityEvent
from src.models.run import Run,TargetRow
from src.utils.serialization import record
def utcnow():return datetime.now(timezone.utc).replace(tzinfo=None)
def ensure():
 with Session() as s:
  if s.get(Capacity,1):return
  s.add(Capacity(id=1,row_limit=TARGET_ROW_LIMIT,used=s.query(TargetRow).count(),reserved=0))
  try:s.commit()
  except IntegrityError:s.rollback()
def free(s):
 cap=s.get(Capacity,1);return cap.row_limit-cap.used-cap.reserved
def acquire_reservation(s,needed):
 if needed<=0:return True
 r=s.execute(update(Capacity).where(Capacity.id==1,Capacity.used+Capacity.reserved+needed<=Capacity.row_limit).values(reserved=Capacity.reserved+needed))
 return r.rowcount==1
def apply_restore_delta(s,delta):
 if delta>0:
  r=s.execute(update(Capacity).where(Capacity.id==1,Capacity.used+Capacity.reserved+delta<=Capacity.row_limit).values(used=Capacity.used+delta))
  return r.rowcount==1
 if delta<0:s.execute(update(Capacity).where(Capacity.id==1).values(used=Capacity.used+delta))
 return True
def release_for_run(s,run_id,reason):
 r=s.execute(update(Reservation).where(Reservation.run_id==run_id,Reservation.status=='active').values(status='released',reason=reason))
 if r.rowcount!=1:return 0
 n=s.query(Reservation.rows).filter_by(run_id=run_id).scalar()
 s.execute(update(Capacity).where(Capacity.id==1).values(reserved=Capacity.reserved-n))
 s.add(CapacityEvent(run_id=run_id,action='release',detail=f'归还预留 {n} 行（{reason}）'))
 return n
def commit_for_run(s,run_id,actual_rows):
 r=s.execute(update(Reservation).where(Reservation.run_id==run_id,Reservation.status=='active').values(status='committed'))
 if r.rowcount!=1:return False
 n=s.query(Reservation.rows).filter_by(run_id=run_id).scalar()
 s.execute(update(Capacity).where(Capacity.id==1).values(reserved=Capacity.reserved-n,used=Capacity.used+actual_rows))
 s.add(CapacityEvent(run_id=run_id,action='commit',detail=f'落库 {actual_rows} 行，预留 {n} 行转为实占'))
 return True
def reclaim_stale(stale_seconds=None):
 ensure()
 cutoff=utcnow()-timedelta(seconds=EXECUTOR_STALE_SECONDS if stale_seconds is None else stale_seconds)
 with Session() as s:
  ids=[x[0] for x in s.query(Run.id).filter(Run.status=='running',(Run.heartbeat_at==None)|(Run.heartbeat_at<cutoff)).all()]
  for run_id in ids:
   r=s.execute(update(Run).where(Run.id==run_id,Run.status=='running').values(status='failed',error='执行器失联，预留额度已回收，请复核后新建任务'))
   if r.rowcount==1:release_for_run(s,run_id,'执行器失联回收')
  s.commit()
def status():
 reclaim_stale()
 with Session() as s:
  cap=s.get(Capacity,1);actual=s.query(TargetRow).count()
  reservations=s.query(Reservation).order_by(Reservation.id.desc()).limit(50).all()
  names={r.id:r.name for r in s.query(Run).filter(Run.id.in_([x.run_id for x in reservations])).all()} if reservations else {}
  events=s.query(CapacityEvent).order_by(CapacityEvent.id.desc()).limit(50).all()
  return {'limit':cap.row_limit,'used':cap.used,'reserved':cap.reserved,'free':cap.row_limit-cap.used-cap.reserved,'actual_target_rows':actual,'stale_seconds':EXECUTOR_STALE_SECONDS,'reservations':[{**record(x),'run_name':names.get(x.run_id,'')} for x in reservations],'events':[record(e) for e in events]}
def set_limit(new_limit):
 reclaim_stale()
 with Session() as s:
  cap=s.get(Capacity,1)
  if new_limit<cap.used:raise HTTPException(409,f'上限不能低于当前实占 {cap.used} 行')
  cap.row_limit=new_limit
  s.add(CapacityEvent(action='limit',detail=f'目标行上限调整为 {new_limit}'))
  s.commit()
 return status()
