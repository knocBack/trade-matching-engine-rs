#!/usr/bin/env python3
"""M3 loopback open-loop harness v1; Python standard library only.

No pending generator queue: offers without a free keep-alive connection are
recorded as generator_capacity failures. Every sample is retained in gzip CSV.
"""
import argparse
import asyncio
import csv
import gzip
import json
import math
import os
import platform
import signal
import subprocess
import time
from pathlib import Path

VERSION = 'm3-http-wall-v1'
SYMBOLS = ['A', 'B', 'C', 'D']

class Connection:
    async def open(self, port):
        self.reader, self.writer = await asyncio.open_connection('127.0.0.1', port)
        self.port = port
        return self

    async def request(self, method, path, body=None):
        data = b'' if body is None else json.dumps(body, separators=(',', ':')).encode()
        self.writer.write((f'{method} {path} HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {len(data)}\r\n\r\n').encode() + data)
        await self.writer.drain()
        headers = await self.reader.readuntil(b'\r\n\r\n')
        lines = headers.decode().split('\r\n')
        status = int(lines[0].split()[1])
        fields = dict(line.lower().split(':', 1) for line in lines[1:] if ':' in line)
        data = await self.reader.readexactly(int(fields['content-length']))
        return status, json.loads(data), len(headers) + len(data)

    async def close(self):
        self.writer.close()
        try:
            await self.writer.wait_closed()
        except OSError:
            pass


def order(symbol, oid, kind='limit', quantity=1, price=0, owner=3):
    value = dict(type=kind, symbol=symbol, order_id=oid, owner_id=owner,
                 side='buy' if kind == 'limit' else 'sell', quantity=quantity)
    if kind == 'limit':
        value['price_ticks'] = price
    return value


def quantiles(values):
    values = sorted(values)
    if not values:
        return None
    return {**{f'p{p}': values[max(0, math.ceil(len(values)*p/100)-1)] for p in [50,90,95,99]}, 'max':values[-1]}


def aggregate(samples, seconds):
    completed = [s for s in samples if s['outcome'] == 'response']
    counts = {}
    for s in samples:
        key = f"{s['outcome']}:{s['status']}:{s['code']}"
        counts[key] = counts.get(key, 0) + 1
    groups = {'all':completed, '200':[s for s in completed if s['status']==200], '503':[s for s in completed if s['status']==503]}
    groups.update({kind:[s for s in completed if s['category']==kind] for kind in ['limit','market','delete']})
    return dict(offered=len(samples),dispatched=sum(s['dispatch'] is not None for s in samples),completed=len(completed),
                http_200=sum(s['status']==200 for s in samples),http_503=sum(s['status']==503 for s in samples),
                domain_errors=sum(s['status'] in [404,409,422] for s in samples),
                timeouts=sum(s['outcome']=='timeout' for s in samples),
                connection_errors=sum(s['outcome']=='connection_error' for s in samples),
                generator_capacity=sum(s['outcome']=='generator_capacity' for s in samples),
                outstanding=0, counts=counts,
                http_wall_ms={k:quantiles([(s['completion']-s['dispatch'])*1000 for s in v]) for k,v in groups.items()},
                scheduling_lag_ms=quantiles([(s['dispatch']-s['scheduled'])*1000 for s in samples if s['dispatch'] is not None]),
                offered_response_ms=quantiles([(s['completion']-s['scheduled'])*1000 for s in completed]),
                successful_rps=sum(s['status']==200 for s in samples)/seconds,
                completed_rps=len(completed)/seconds,
                missing_fraction=1-len(completed)/len(samples),
                rejection_fraction=sum(s['status']!=200 for s in completed)/len(samples))

async def run(args):
    out = Path(args.output)
    out.mkdir(parents=True, exist_ok=True)
    binary = str(Path(args.server).resolve())
    invocation = [binary,'--listen','127.0.0.1:0','--symbols',','.join(SYMBOLS),'--queue-capacity',str(args.queue),'--in-flight',str(args.in_flight),'--connections','1024','--body-limit','16384']
    process = await asyncio.create_subprocess_exec(*invocation,stdout=asyncio.subprocess.PIPE,stderr=asyncio.subprocess.PIPE)
    clients = []
    try:
        ready = json.loads(await process.stdout.readline())
        port = int(ready['listening'].split(':')[-1])
        clients = await asyncio.gather(*(Connection().open(port) for _ in range(args.clients)))
        free = clients.copy()
        offers = int(args.rate*(args.warmup+args.duration))
        pool = math.ceil(math.ceil(offers/16)*1.1)
        # Each symbol needs its own market and protected cancellation pools.
        preload = [(s, i+1,100) for s in SYMBOLS for i in range(pool)] + [(s,1_000_000+i,-100) for s in SYMBOLS for i in range(pool)]
        async def preload_worker(c, offset):
            for s,oid,tick in preload[offset::len(clients)]:
                status,body,_ = await c.request('POST','/orders',order(s,oid,price=tick,owner=1))
                assert status == 200, (status, body)
        await asyncio.gather(*(preload_worker(c,i) for i,c in enumerate(clients)))
        async def snapshots(name):
            values = {}
            for s in SYMBOLS:
                status,body,_ = await clients[0].request('GET',f'/book?symbol={s}')
                assert status == 200
                values[s] = body
            with gzip.open(out/f'{name}.json.gz','wt') as f:
                json.dump(values,f,separators=(',',':'))
            return values
        before = await snapshots('preload')
        for s in SYMBOLS:
            assert sum(len(l['orders']) for l in before[s]['bids']) == pool*2
        samples = []
        pending = set()
        cancels = [0]*4
        origin = time.perf_counter()
        warm_count = int(args.rate*args.warmup)
        async def dispatch(c, sample, method,path,body):
            try:
                status,result,nbytes = await asyncio.wait_for(c.request(method,path,body),args.timeout)
                sample.update(status=status,code=result.get('error',{}).get('code',''),bytes=nbytes,outcome='response')
            except asyncio.TimeoutError:
                sample['outcome']='timeout'
                await c.close()
                await c.open(port)
            except (OSError,asyncio.IncompleteReadError,ValueError,KeyError) as error:
                sample.update(outcome='connection_error',code=type(error).__name__)
                await c.close()
                await c.open(port)
            finally:
                sample['completion']=time.perf_counter()-origin
                free.append(c)
        for i in range(offers):
            scheduled=i/args.rate
            delay=origin+scheduled-time.perf_counter()
            if delay>0:
                await asyncio.sleep(delay)
            symbol=SYMBOLS[i%4]
            category=['limit','limit','market','delete'][(i//4)%4]
            sample=dict(index=i,phase='warmup' if i<warm_count else 'measurement',scheduled=scheduled,dispatch=None,completion=None,
                        category=category,endpoint='DELETE' if category=='delete' else 'POST',status=0,code='',bytes=0,outcome='generator_capacity')
            samples.append(sample)
            if category=='delete':
                oid=1_000_000+cancels[i%4]
                cancels[i%4]+=1
                method,path,body='DELETE',f'/orders/{oid}?symbol={symbol}',None
            else:
                method,path,body='POST','/orders',order(symbol,(2_000_000 if category=='limit' else 3_000_000)+i,kind='limit' if category=='limit' else 'market')
            if not free:
                continue
            c=free.pop()
            sample['dispatch']=time.perf_counter()-origin
            task=asyncio.create_task(dispatch(c,sample,method,path,body))
            pending.add(task)
            task.add_done_callback(pending.discard)
        await asyncio.gather(*pending)  # drain every dispatched client attempt
        drained=time.perf_counter()-origin
        after=await snapshots('final')
        await asyncio.gather(*(c.close() for c in clients))
        clients=[]
        process.send_signal(signal.SIGTERM)
        stdout,stderr=await process.communicate()
        (out/'server.stderr.txt').write_bytes(stderr)
        (out/'server.stdout.txt').write_bytes(json.dumps(ready).encode()+b'\n'+stdout)
        assert process.returncode==0,stderr
        metrics=json.loads(stdout.splitlines()[-1])
        with gzip.open(out/'samples.csv.gz','wt',newline='') as f:
            writer=csv.DictWriter(f,fieldnames=list(samples[0]))
            writer.writeheader();writer.writerows(samples)
        measured=[s for s in samples if s['phase']=='measurement']
        summary=aggregate(measured,args.duration)
        warm=aggregate(samples[:warm_count],args.warmup) if warm_count else None
        summary['gate_demonstrated']=(summary['missing_fraction']<.1 and summary['http_wall_ms']['all'] is not None and summary['http_wall_ms']['all']['p90']<=1000)
        summary['server']=metrics
        summary['warmup']=warm
        summary['drained_seconds']=drained
        summary['final_orders']={s:sum(len(l['orders']) for l in after[s]['bids']+after[s]['asks']) for s in SYMBOLS}
        summary['reconciliation']={'commands_admitted_minus_completed':metrics['commands_admitted']-metrics['commands_completed'],
                                   'preload_commands':len(preload),'load_admitted':metrics['commands_admitted']-len(preload),
                                   'load_engine_completed':metrics['commands_completed']-len(preload),
                                   'load_http_200':sum(s['status']==200 for s in samples),
                                   'load_admitted_rps':(metrics['commands_admitted']-len(preload))/(args.warmup+args.duration),
                                   'load_engine_completed_rps':(metrics['commands_completed']-len(preload))/(args.warmup+args.duration)}
        summary['configuration']={**vars(args),'invocation':invocation,'pool_per_symbol':pool,'ticks':{'match':100,'cancel':-100,'new':0},'version':VERSION}
        summary['environment']={'python':platform.python_version(),'os':platform.platform(),'logical_cpus':os.cpu_count(),
                                'commit':subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip(),
                                'git_status':subprocess.check_output(['git','status','--porcelain'],text=True),
                                'rust':subprocess.check_output(['rustc','--version'],text=True).strip()}
        (out/'summary.json').write_text(json.dumps(summary,indent=2)+'\n')
        print(json.dumps({'output':str(out),'summary':summary},indent=2),flush=True)
    finally:
        for c in clients:
            await c.close()
        if process.returncode is None:
            process.send_signal(signal.SIGTERM)
            await process.communicate()

if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--server',default='target/release/trade-http')
    p.add_argument('--output',required=True)
    p.add_argument('--rate',type=int,default=500)
    p.add_argument('--warmup',type=float,default=30)
    p.add_argument('--duration',type=float,default=120)
    p.add_argument('--clients',type=int,default=128)
    p.add_argument('--timeout',type=float,default=10)
    p.add_argument('--queue',type=int,default=256)
    p.add_argument('--in-flight',type=int,default=512)
    args=p.parse_args()
    if min(args.rate,args.duration,args.clients,args.timeout,args.queue,args.in_flight)<=0 or args.warmup<0:
        p.error('positive values required; warmup may be zero')
    asyncio.run(run(args))
