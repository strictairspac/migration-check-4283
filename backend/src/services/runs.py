import redis
from datetime import datetime,timezone
from fastapi import HTTPException
from sqlalchemy import update
from src.config.database import Session
from src.config.settings import REDIS_URL
from src.models.batch import Batch
from src.models.mapping import Mapping
from src.models.run import Run,TargetRow
from src.models.capacity import Reservation,CapacityEvent
from src.utils.serialization import record
from src.services.migration import digest,key_digest,validate_backup,migrate
from src.services import capacity
queue=redis.Redis.from_url(REDIS_URL)
def list_items():
 capacity.reclaim_stale()
 with Session() as s:return [record(r) for r in s.query(Run).order_by(Run.id.desc()).limit(100).all()]
def detail(item_id):
 with Session() as s:
  r=s.get(Run,item_id)
  if not r:raise HTTPException(404,'未找到对账运行')
  return record(r)
def create(data):
 capacity.reclaim_stale()
 with Session() as s:
  m=s.get(Mapping,data.mapping_id)
  if not m:raise HTTPException(404,'未找到字段映射')
  b=s.get(Batch,m.batch_id)
  snapshot={'batch':record(b),'mapping':record(m)}
  try:needed=migrate(b.rows,m.rules,m.chunk_size)['accepted_count']
  except ValueError as e:raise HTTPException(422,str(e))
  batch_id,mapping_id=b.id,m.id
 with Session() as s:
  if not capacity.acquire_reservation(s,needed):
   free=capacity.free(s)
   s.add(CapacityEvent(action='reject',detail=f'拒绝受理「{data.name}」：需预留 {needed} 行，可用 {free} 行，还差 {needed-free} 行'))
   s.commit()
   raise HTTPException(409,f'目标库容量不足：本次需预留 {needed} 行，当前可用 {free} 行，还差 {needed-free} 行')
  r=Run(name=data.name,batch_id=batch_id,mapping_id=mapping_id,status='queued',snapshot=snapshot)
  s.add(r);s.flush()
  s.add(Reservation(run_id=r.id,rows=needed,status='active'))
  s.add(CapacityEvent(run_id=r.id,action='reserve',detail=f'受理「{data.name}」，预留 {needed} 行目标位置（坏行与同键重复不占额度）'))
  s.commit();s.refresh(r);result=record(r)
 try:queue.rpush('migration-runs',result['id'])
 except redis.RedisError:
  with Session() as s:
   s.execute(update(Run).where(Run.id==result['id'],Run.status=='queued').values(status='failed',error='队列不可用，请创建新任务'))
   capacity.release_for_run(s,result['id'],'队列不可用')
   s.commit()
  raise HTTPException(503,'队列不可用，请稍后重试')
 return result
def cancel(item_id):
 with Session() as s:
  r=s.query(Run).filter_by(id=item_id).with_for_update().first()
  if not r:raise HTTPException(404,'未找到对账运行')
  if r.status not in ('queued','running'):raise HTTPException(409,'运行已终止，无法取消')
  r.cancel_requested=True
  if r.status=='queued':
   r.status='cancelled'
   capacity.release_for_run(s,item_id,'受理后取消')
  s.commit();return record(r)
def targets(item_id):
 detail(item_id)
 with Session() as s:rows=[r.payload for r in s.query(TargetRow).filter_by(run_id=item_id).order_by(TargetRow.ordinal)]
 return {'rows':rows,'count':len(rows),'checksum':digest(rows)}
def backup(item_id):
 r=detail(item_id)
 if r['status']!='completed':raise HTTPException(409,'仅完成运行可以备份')
 return r['result']['backup']
def restore(item_id,data):
 capacity.ensure()
 with Session() as s:
  r=s.query(Run).filter_by(id=item_id).with_for_update().first()
  if not r:raise HTTPException(404,'未找到对账运行')
  if r.status!='completed':raise HTTPException(409,'仅完成运行可以恢复')
  try:rows=validate_backup(data.backup,r.result['checksum'],item_id)
  except ValueError as e:raise HTTPException(422,str(e))
  before=s.query(TargetRow).filter_by(run_id=item_id).count()
  delta=len(rows)-before
  if not capacity.apply_restore_delta(s,delta):
   free=capacity.free(s)
   s.add(CapacityEvent(run_id=item_id,action='reject',detail=f'拒绝恢复运行 #{item_id}：净增 {delta} 行，可用 {free} 行，还差 {delta-free} 行'))
   s.commit()
   raise HTTPException(409,f'目标库容量不足：恢复净增 {delta} 行，当前可用 {free} 行，还差 {delta-free} 行')
  s.query(TargetRow).filter_by(run_id=item_id).delete()
  for i,row in enumerate(rows):s.add(TargetRow(run_id=item_id,ordinal=i,key_digest=key_digest(row),payload=row))
  s.flush()
  actual=[x.payload for x in s.query(TargetRow).filter_by(run_id=item_id).order_by(TargetRow.ordinal)]
  proof={'restored_at':datetime.now(timezone.utc).isoformat(),'previous_count':before,'restored_count':len(actual),'checksum':digest(actual),'verified':digest(actual)==r.result['checksum']}
  if not proof['verified']:raise HTTPException(409,'恢复后校验失败，已回滚')
  r.recovery=(r.recovery or [])+[proof]
  s.add(CapacityEvent(run_id=item_id,action='restore',detail=f'恢复运行 #{item_id} 目标 {len(rows)} 行，净增 {delta} 行'))
  s.commit();return proof
