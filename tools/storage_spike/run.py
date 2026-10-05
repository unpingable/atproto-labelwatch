"""Durable, finite, campaign-owned producer. Launch through user-systemd."""
from __future__ import annotations
import datetime
import argparse
import json
import os
import subprocess
import sys
import time
import traceback
from pathlib import Path
from common import allocated,atomic,guard_root

ROOT=Path('/data/git/atproto-nutrition/portfolio-private/campaigns/labelwatch-storage-architecture-20261005')
IMAGE='postgres@sha256:639ab7ceb90e13123085b741fb31ef493fba25463002f6da665352e7b534b652'
CONTAINER='labelwatch-storage-pg-abf37f3e'

def docker(*args,**kwargs):return subprocess.run(['docker',*args],check=True,**kwargs)

def admission():
    record={p:{'available_bytes':os.statvfs(p).f_bavail*os.statvfs(p).f_frsize,'available_inodes':os.statvfs(p).f_favail} for p in ('/','/data')}
    for p,budget in (('/',1073741824),('/data',17179869184)):
        if record[p]['available_bytes']-budget<64424509440:raise RuntimeError('shared host storage reserve refused')
    atomic(ROOT/'evidence/EXECUTION-ADMISSION.json',{'time':datetime.datetime.now(datetime.timezone.utc).isoformat(),'filesystems':record,'new_data_ceiling_bytes':17179869184,'reserve_each_bytes':64424509440,'owner':'Codex /root','other_tenant_allocation':'Future allocation unknown; periodic reserve monitor required'})

def main():
    runtime=guard_root(ROOT/'runtime');os.umask(0o077);admission()
    socket_parent=Path('/data/git/.lane-sockets');socket_parent.mkdir(mode=0o700,exist_ok=True)
    socket=socket_parent/CONTAINER;socket.mkdir(mode=0o777);socket.chmod(0o777)
    state=runtime/'pgstate';state.mkdir()
    os.environ['SPIKE_PG_SOCKET']=str(socket)
    docker('run','-d','--name',CONTAINER,'--network','none','--cpus','2','--memory','2g',
           '--mount',f'type=bind,src={state},dst=/var/lib/postgresql/data',
           '--mount',f'type=bind,src={socket},dst=/var/run/postgresql',
           '-e','POSTGRES_HOST_AUTH_METHOD=trust','-e','PGHOST=/var/run/postgresql',IMAGE,
           'postgres','-c','listen_addresses=','-c','unix_socket_directories=/var/run/postgresql',
           '-c','unix_socket_permissions=0777','-c','max_wal_size=1GB')
    import psycopg
    for _ in range(120):
        try:
            c=psycopg.connect(host=str(socket),dbname='postgres',user='postgres',autocommit=True);break
        except psycopg.OperationalError as e:
            if _==0:print('PostgreSQL readiness: '+str(e),flush=True)
            time.sleep(1)
    else:raise RuntimeError('owned PostgreSQL failed to start')
    version=c.execute('SELECT version()').fetchone()[0];c.execute('CREATE DATABASE fixtures');c.close()
    identity=docker('inspect',CONTAINER,'--format','{{json .Id}}',capture_output=True,text=True).stdout.strip()
    atomic(ROOT/'evidence/POSTGRES-IDENTITY.json',{'container':CONTAINER,'id':json.loads(identity),'image':IMAGE,'version':version,'network':'none; no TCP port; private socket under owner-only campaign parent','state':str(state),'socket':str(socket),'credentials':'fixture-local trust only, no production accounts','fsync':'on (default)','cpus':2,'memory_max':'2GiB'})
    from qualify import qualifies
    qualifies(runtime)
    # Actual owned engine process loss/restart; no production service touched.
    c=psycopg.connect(host=str(socket),dbname='fixtures',user='postgres',autocommit=True)
    before=c.execute('SELECT COUNT(*) FROM pg_namespace WHERE nspname LIKE %s',('postgres_%',)).fetchone()[0]
    c.close();docker('kill','--signal','KILL',CONTAINER);docker('start',CONTAINER)
    for _ in range(120):
        try:c=psycopg.connect(host=str(socket),dbname='fixtures',user='postgres');break
        except psycopg.OperationalError:time.sleep(1)
    after=c.execute('SELECT COUNT(*) FROM pg_namespace WHERE nspname LIKE %s',('postgres_%',)).fetchone()[0];c.close()
    assert before==after
    atomic(ROOT/'evidence/POSTGRES-RESTART.json',{'scope':'owned PostgreSQL process SIGKILL and restart, NOT actual host reboot','namespace_count_before':before,'namespace_count_after':after,'parity':'PASS'})
    from benchmark import main as benchmark
    sys.argv=[sys.argv[0],str(runtime)];benchmark()
    docker('stop','--time','30',CONTAINER)

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--occurrence',type=Path);parser.add_argument('--container')
    args=parser.parse_args()
    if args.occurrence:
        ROOT=guard_root(args.occurrence);ROOT.mkdir();(ROOT/'evidence').mkdir();(ROOT/'runtime').mkdir()
    if args.container:
        if not args.container.startswith('labelwatch-storage-pg-'):raise ValueError('campaign container prefix required')
        CONTAINER=args.container
    os.environ['SPIKE_CONTAINER']=CONTAINER
    result={'started_at':datetime.datetime.now(datetime.timezone.utc).isoformat()}
    try:main();result['status']='PASS'
    except BaseException as e:
        result.update(status='FAILED',error=type(e).__name__+': '+str(e));traceback.print_exc();raise
    finally:
        result['finished_at']=datetime.datetime.now(datetime.timezone.utc).isoformat()
        atomic(ROOT/'evidence/PRODUCER-TERMINAL.json',result)
