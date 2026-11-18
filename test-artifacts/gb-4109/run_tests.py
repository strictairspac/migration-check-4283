#!/usr/bin/env python3
"""gb-4109 容量台账场景验证：目标行上限 / 受理预留 / 并发争抢 / 失联回收 / 满库恢复。
SQLite 落盘于本目录，Redis 队列以内存假件替代；不污染源码树。运行：python3 run_tests.py"""
import os,sys,time,threading
ART=os.path.dirname(os.path.abspath(__file__))
BACKEND=os.path.abspath(os.path.join(ART,'..','..','backend'))
DB=os.path.join(ART,'capacity-test.db')
if os.path.exists(DB):os.remove(DB)
os.environ['DATABASE_URL']='sqlite:///'+DB+'?check_same_thread=false'
os.environ['REDIS_URL']='redis://127.0.0.1:6399/0'
os.environ['JWT_SECRET']='gb4109-test-secret'
os.environ['TARGET_ROW_LIMIT']='1000'
os.environ['EXECUTOR_STALE_SECONDS']='120'
os.environ['SEED_DEMO']='false'
sys.path.insert(0,BACKEND)
from fastapi import HTTPException
from src.config.database import Base,engine,Session
from src.models.identity import User,Audit
from src.models.batch import Batch
from src.models.mapping import Mapping
from src.models.run import Run,TargetRow
from src.models.capacity import Capacity,Reservation,CapacityEvent
from src.schemas.business import RunInput,RestoreInput
from src.services import capacity
from src.services import runs as runs_svc
import src.worker as worker
from src.utils.security import hash_password
class FakeQueue:
 def __init__(self):self.items=[]
 def rpush(self,*a):self.items.append(a);return len(self.items)
 def ping(self):return True
runs_svc.queue=FakeQueue()
Base.metadata.create_all(engine)
RULES=[{'source':a,'target':b,'type':c,'required':True} for a,b,c in [('委托号','sample_code','text'),('检测项','test_name','text'),('结果','result_value','decimal'),('单位','unit','text'),('时间','measured_at','datetime')]]
FAILED=[]
def check(label,cond,info=''):
 print(('  PASS ' if cond else '  FAIL ')+label+((' | '+str(info)) if not cond and info else ''))
 if not cond:FAILED.append(label)
def reset(limit=1000):
 with Session() as s:
  for m in (TargetRow,Reservation,CapacityEvent,Run,Mapping,Batch,Audit):s.query(m).delete()
  cap=s.get(Capacity,1)
  if cap:cap.used=0;cap.reserved=0;cap.row_limit=limit
  else:s.add(Capacity(id=1,row_limit=limit,used=0,reserved=0))
  s.commit()
def good(n,prefix='JS'):
 return [{'委托号':f'{prefix}-{i:03d}','检测项':'拉伸强度','结果':str(420+i),'单位':'MPa','时间':'2026-11-17T09:00:00+08:00'} for i in range(1,n+1)]
def make_input(name,rows,chunk=4):
 with Session() as s:
  b=Batch(name=name,source_file='t.json',description='',rows=rows);s.add(b);s.flush()
  m=Mapping(name=name+'-map',batch_id=b.id,rules=RULES,chunk_size=chunk,note='');s.add(m);s.commit()
  return m.id
def create_run(name,mid):return runs_svc.create(RunInput(name=name,mapping_id=mid))
def cap():return capacity.status()
def t1():
 print('T1 受理即预留；容量不足拒绝并说明差额')
 reset(limit=10)
 mid=make_input('t1-batch',good(6))
 create_run('t1-run',mid)
 st=cap()
 check('T1 受理即预留6行，可用4行',st['reserved']==6 and st['free']==4,f"reserved={st['reserved']} free={st['free']}")
 mid2=make_input('t1-batch2',good(7))
 try:create_run('t1-run2',mid2);msg=''
 except HTTPException as e:msg=str(e.detail) if e.status_code==409 else ''
 check('T1 拒绝受理并说明差多少',msg!='' and '需预留 7 行' in msg and '可用 4 行' in msg and '还差 3 行' in msg,msg)
 evs=[e for e in cap()['events'] if e['action']=='reject']
 check('T1 拒绝原因已入容量事件供页面核对',bool(evs) and '还差 3 行' in evs[0]['detail'],evs)
def t2():
 print('T2 坏行与同键重复行不占额度')
 reset(limit=4)
 rows=good(4,'T2')+[good(4,'T2')[0],
  {'委托号':'T2-BAD','检测项':'拉伸强度','结果':'四百二十','单位':'MPa','时间':'2026-11-17T09:00:00+08:00'},
  {'委托号':'T2-NULL','检测项':'拉伸强度','结果':None,'单位':'MPa','时间':'2026-11-17T09:00:00+08:00'},
  {'委托号':'T2-MISS','检测项':'拉伸强度','结果':'1','时间':'2026-11-17T09:00:00+08:00'}]
 mid=make_input('t2-batch',rows)
 create_run('t2-run',mid)
 st=cap()
 check('T2 8行源仅预留4行（重复键/非法数/空值/缺列不占额）',st['reserved']==4 and st['free']==0,f"reserved={st['reserved']} free={st['free']}")
def t3():
 print('T3 仅剩一份时并发争抢')
 reset(limit=5)
 mid=make_input('t3-batch',good(5))
 results=[];lock=threading.Lock()
 def attempt(i):
  try:
   r=create_run(f't3-run-{i}',mid)
   with lock:results.append(('ok',r['name']))
  except HTTPException as e:
   with lock:results.append(('err',e.status_code,str(e.detail)))
 ts=[threading.Thread(target=attempt,args=(i,)) for i in range(4)]
 [t.start() for t in ts];[t.join() for t in ts]
 oks=[r for r in results if r[0]=='ok'];errs=[r for r in results if r[0]=='err']
 check('T3 恰有一个受理成功，其余被拒',len(oks)==1 and len(errs)==3,results)
 check('T3 失败者均409且说明差额',all(e[1]==409 and '还差' in e[2] for e in errs),errs)
 st=cap()
 check('T3 并发受理未超额：预留5可用0',st['reserved']==5 and st['free']==0,f"reserved={st['reserved']} free={st['free']}")
def t4():
 print('T4 落库时预留转为实占')
 reset(limit=20)
 rows=good(7)+[good(7)[0],{'委托号':'JS-BAD','检测项':'拉伸强度','结果':'四百二十','单位':'MPa','时间':'2026-11-17T09:00:00+08:00'}]
 mid=make_input('t4-batch',rows)
 r=create_run('t4-run',mid)
 st=cap()
 check('T4 受理预留7行',st['reserved']==7,f"reserved={st['reserved']}")
 worker.handle(r['id'])
 d=runs_svc.detail(r['id']);st=cap()
 check('T4 完成后预留转实占',d['status']=='completed' and st['used']==7 and st['reserved']==0,f"status={d['status']} used={st['used']} reserved={st['reserved']}")
 check('T4 台账与目标表实际行数一致',st['actual_target_rows']==7,f"actual={st['actual_target_rows']}")
 check('T4 拒绝区保留重复键与坏行',d['result']['rejected_count']==2,f"rejected={d['result']['rejected_count']}")
def t5():
 print('T5 取消归还额度')
 reset(limit=10)
 mid=make_input('t5-batch',good(5))
 r=create_run('t5-run',mid)
 runs_svc.cancel(r['id'])
 d=runs_svc.detail(r['id']);st=cap()
 check('T5 排队取消后预留归还',d['status']=='cancelled' and st['reserved']==0,f"status={d['status']} reserved={st['reserved']}")
 try:runs_svc.cancel(r['id']);again=False
 except HTTPException as e:again=e.status_code==409
 check('T5 重复取消拒绝且不重复归还',again and cap()['reserved']==0)
def t6():
 print('T6 执行失败归还额度')
 reset(limit=10)
 mid=make_input('t6-batch',good(5))
 r=create_run('t6-run',mid)
 orig=worker.migrate
 def boom(*a,**k):raise RuntimeError('boom')
 worker.migrate=boom
 try:worker.handle(r['id'])
 finally:worker.migrate=orig
 d=runs_svc.detail(r['id']);st=cap()
 check('T6 失败归还预留',d['status']=='failed' and st['reserved']==0 and st['used']==0,f"status={d['status']} reserved={st['reserved']} used={st['used']}")
def t7():
 print('T7 执行失联回收 + 迟到的旧执行者回来')
 reset(limit=10)
 mid=make_input('t7-batch',good(5))
 r=create_run('t7-run',mid);rid=r['id']
 with Session() as s:
  run=s.get(Run,rid);run.status='running';run.heartbeat_at=capacity.utcnow();s.commit()
 time.sleep(0.02)
 capacity.reclaim_stale(stale_seconds=0)
 st=cap();d=runs_svc.detail(rid)
 check('T7 失联后运行置失败并回收预留',d['status']=='failed' and '失联' in (d['error'] or '') and st['reserved']==0,f"status={d['status']} err={d['error']} reserved={st['reserved']}")
 fake={'accepted_rows':[{'sample_code':'JS-001','test_name':'拉伸强度','result_value':'421.000000','unit':'MPa','measured_at':'2026-11-17T01:00:00+00:00'}]}
 try:worker.finalize(rid,fake);late=False
 except worker.Lost:late=True
 check('T7 迟到执行者落库被拒绝',late)
 with Session() as s:n=s.query(TargetRow).filter_by(run_id=rid).count()
 check('T7 迟到执行者未留下目标行，额度不变',n==0 and cap()['used']==0,f"rows={n} used={cap()['used']}")
 try:worker.progress(rid,{'chunk':1});prog=False
 except worker.Lost:prog=True
 check('T7 迟到执行者心跳上报被拒绝',prog)
 worker.handle(rid)
 check('T7 失败运行不会被重新接走',runs_svc.detail(rid)['status']=='failed')
def t8():
 print('T8 重试不重复占额')
 reset(limit=10)
 mid=make_input('t8-batch',good(5))
 r=create_run('t8-run',mid)
 worker.handle(r['id'])
 with Session() as s:again_commit=capacity.commit_for_run(s,r['id'],5);s.commit()
 with Session() as s:again_release=capacity.release_for_run(s,r['id'],'重试');s.commit()
 capacity.reclaim_stale(stale_seconds=0)
 st=cap()
 check('T8 重复提交/释放/回收均幂等',(not again_commit) and again_release==0 and st['used']==5 and st['reserved']==0,f"used={st['used']} reserved={st['reserved']}")
 bk=runs_svc.detail(r['id'])['result']['backup']
 p1=runs_svc.restore(r['id'],RestoreInput(backup=bk))
 p2=runs_svc.restore(r['id'],RestoreInput(backup=bk))
 check('T8 恢复重试净增为0不重复占额',p1['verified'] and p2['verified'] and cap()['used']==5,f"used={cap()['used']}")
def t9():
 print('T9 满库恢复与上限校验')
 reset(limit=7)
 mid=make_input('t9-batch',good(7))
 r=create_run('t9-run',mid)
 worker.handle(r['id'])
 st=cap()
 check('T9 满库：实占7可用0',st['used']==7 and st['free']==0,f"used={st['used']} free={st['free']}")
 mid2=make_input('t9-batch2',good(1,'T9B'))
 try:create_run('t9-run2',mid2);msg=''
 except HTTPException as e:msg=str(e.detail) if e.status_code==409 else ''
 check('T9 满库新受理拒绝并说明差额','还差 1 行' in msg,msg)
 bk=runs_svc.detail(r['id'])['result']['backup']
 p=runs_svc.restore(r['id'],RestoreInput(backup=bk))
 st=cap()
 check('T9 满库等量替换恢复成功',p['verified'] and p['restored_count']==7 and st['used']==7 and st['free']==0,f"proof={p} used={st['used']}")
 evs=[e for e in cap()['events'] if e['action']=='restore']
 check('T9 恢复事件记录净增0行',bool(evs) and '净增 0 行' in evs[0]['detail'],evs)
 try:capacity.set_limit(6);low=False
 except HTTPException as e:low=e.status_code==409
 check('T9 上限不得低于当前实占',low)
 capacity.set_limit(7)
 mid3=make_input('t9-batch3',[])
 r3=create_run('t9-run3',mid3)
 worker.handle(r3['id'])
 d3=runs_svc.detail(r3['id']);st=cap()
 check('T9 空批次净增0满库可受理完成',d3['status']=='completed' and st['used']==7,f"status={d3['status']} used={st['used']}")
def t10():
 print('T10 API 冒烟：权限 / 拒绝报文 / 容量页数据')
 reset()
 from src.main import app
 from fastapi.testclient import TestClient
 with TestClient(app) as client:
  def login(email):
   r=client.post('/auth/login',json={'email':email,'password':'Migrate2026!'})
   return {'Authorization':'Bearer '+r.json()['token']} if r.status_code==200 else {}
  admin=login('admin@migration.local');op=login('operator@migration.local');vw=login('viewer@migration.local')
  check('T10 三账号可登录',all([admin,op,vw]))
  check('T10 健康检查通过',client.get('/health').status_code==200)
  mid=make_input('t10-batch',good(5))
  check('T10 非管理员不能调上限',client.put('/capacity',json={'limit':3},headers=op).status_code==403)
  r=client.put('/capacity',json={'limit':3},headers=admin)
  check('T10 管理员调整上限',r.status_code==200 and r.json()['limit']==3,r.text[:200])
  r=client.post('/runs',json={'name':'t10-run','mapping_id':mid},headers=op)
  check('T10 超额受理409且说明差额',r.status_code==409 and '还差 2 行' in r.json()['detail'],r.text[:200])
  r=client.get('/capacity',headers=vw)
  j=r.json() if r.status_code==200 else {}
  ok=j.get('limit')==3 and j.get('used')==0 and j.get('reserved')==0 and any(e['action']=='reject' and '还差 2 行' in e['detail'] for e in j.get('events',[]))
  check('T10 容量页可核对上限/实占/预留/拒绝原因',ok,str(j)[:300])
  check('T10 观察员可查看运行列表',client.get('/runs',headers=vw).status_code==200)
def setup_users():
 with Session() as s:
  for role in ('admin','operator','viewer'):
   if not s.query(User).filter_by(email=role+'@migration.local').first():s.add(User(email=role+'@migration.local',role=role,password=hash_password('Migrate2026!')))
  s.commit()
if __name__=='__main__':
 setup_users()
 for t in (t1,t2,t3,t4,t5,t6,t7,t8,t9,t10):
  try:t()
  except Exception as e:
   import traceback;traceback.print_exc();FAILED.append(t.__name__+' 异常:'+str(e))
 print('='*60)
 print(f'结果：{"全部通过" if not FAILED else "存在失败"}（失败 {len(FAILED)} 项）')
 for f in FAILED:print('  FAIL',f)
 sys.exit(1 if FAILED else 0)
