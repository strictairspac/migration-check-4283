import time,logging,redis
from pathlib import Path
from sqlalchemy import update
from src.config.settings import REDIS_URL
from src.config.database import Session
from src.models.run import Run,TargetRow
from src.services.migration import migrate,make_backup,key_digest,Cancelled
from src.services.capacity import utcnow,commit_for_run,release_for_run
from src.services.audit import log
q=redis.Redis.from_url(REDIS_URL)
class Lost(Exception):pass
def progress(run_id,info):
 Path('/tmp/worker-heartbeat').touch()
 with Session() as s:
  r=s.get(Run,run_id)
  if r.cancel_requested or r.status=='cancelled':raise Cancelled()
  if r.status!='running':raise Lost()
  r.progress=info;r.heartbeat_at=utcnow();s.commit()
def finalize(run_id,result):
 with Session() as s:
  r=s.query(Run).filter_by(id=run_id).with_for_update().first()
  if not r or r.status!='running':raise Lost()
  if r.cancel_requested:raise Cancelled()
  if not commit_for_run(s,run_id,len(result['accepted_rows'])):raise Lost()
  for i,row in enumerate(result['accepted_rows']):s.add(TargetRow(run_id=run_id,ordinal=i,key_digest=key_digest(row),payload=row))
  r.result=result;r.status='completed';s.commit()
def handle(run_id):
 with Session() as s:
  r=s.query(Run).filter_by(id=run_id).with_for_update().first()
  if not r or r.status!='queued':return
  r.status='running';r.heartbeat_at=utcnow();snapshot=r.snapshot;s.commit()
 try:
  result=migrate(snapshot['batch']['rows'],snapshot['mapping']['rules'],snapshot['mapping']['chunk_size'],lambda info:progress(run_id,info))
  result['backup']=make_backup(result,run_id,snapshot['batch']['name'])
  finalize(run_id,result)
  log('worker','run.completed',str(run_id))
 except Cancelled:
  with Session() as s:
   r=s.execute(update(Run).where(Run.id==run_id,Run.status=='running').values(status='cancelled',error='用户取消，未提交目标行'))
   if r.rowcount==1:release_for_run(s,run_id,'运行取消')
   s.commit()
  log('worker','run.cancelled',str(run_id))
 except Lost:
  log('worker','run.lost',str(run_id))
 except Exception as exc:
  logging.exception('migration failed')
  with Session() as s:
   r=s.execute(update(Run).where(Run.id==run_id,Run.status=='running').values(status='failed',error=str(exc)[:1000]))
   if r.rowcount==1:release_for_run(s,run_id,'执行失败')
   s.commit()
def main():
 while True:
  Path('/tmp/worker-heartbeat').touch()
  try:
   item=q.blpop('migration-runs',timeout=2)
   if not item:continue
   handle(int(item[1]))
  except Exception:logging.exception('worker loop');time.sleep(2)
if __name__=='__main__':main()
