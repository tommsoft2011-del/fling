#!/usr/bin/env python3
"""Run the actual Linux debug app twice over production LiveKit + native DC.
Same host only: not WAN or Windows validation. No credentials in arguments/logs.
"""
import argparse, hashlib, json, os, pathlib, signal, subprocess, time, uuid

p=argparse.ArgumentParser()
p.add_argument('--binary',required=True)
p.add_argument('--output',required=True)
p.add_argument('--cases',default='small,large,repeat,empty,cancel,retry')
p.add_argument('--expected-build-id',required=True)
a=p.parse_args();root=pathlib.Path(a.output).resolve();root.mkdir(parents=True,exist_ok=True)
source=root/'source';source.mkdir(exist_ok=True)
block=bytes(range(256))*4096
sizes={'small':1024*127+17,'large':64*1024*1024+19,'repeat':1024*127+17,'empty':0,'cancel':128*1024*1024+31,'retry':1024*1024+7,'turn':4*1024*1024+13}
results=[]
for case in a.cases.split(','):
    size=sizes[case];name='repeat.bin' if case in ('small','repeat') else case+'.bin'
    path=source/name
    with path.open('wb') as f:
        left=size
        while left:n=min(left,len(block));f.write(block[:n]);left-=n
        f.flush();os.fsync(f.fileno())
    digest=hashlib.sha256(path.read_bytes()).hexdigest()
    case_dir=root/case;case_dir.mkdir(exist_ok=True)
    output=root/'received' if case in ('small','repeat') else case_dir/'received';output.mkdir(exist_ok=True)
    if case=='repeat':
        existing=output/name
        if not existing.exists():raise RuntimeError('repeat needs small case first')
        before=hashlib.sha256(existing.read_bytes()).hexdigest()
    else:before=None
    task=uuid.uuid4().hex
    specs=[]
    for role in ('receiving','sending'):
        spec={'force_turn':case=='turn','role':role,'task':task,'name':name,'size':size,'source':str(path) if role=='sending' else None,'output':str(output),'log':str(case_dir/(role+'.jsonl')),'cancel_after_bytes':1048576 if case=='cancel' and role=='receiving' else None}
        spec_path=case_dir/(role+'.json');spec_path.write_text(json.dumps(spec));specs.append(spec_path)
    processes=[];files=[];started=time.monotonic()
    try:
        for idx,role in enumerate(('receiving','sending')):
            log=(case_dir/(role+'.stderr')).open('w');files.append(log)
            env=os.environ.copy();env['WEBKIT_DISABLE_SANDBOX_THIS_IS_DANGEROUS']='1'
            proc=subprocess.Popen(['xvfb-run','-a',a.binary,'--internal-native-acceptance',str(specs[idx])],stdout=log,stderr=subprocess.STDOUT,env=env,start_new_session=True)
            processes.append(proc)
            if idx==0:time.sleep(2)
        codes=[]
        for proc in processes:
            codes.append(proc.wait(timeout=max(1,110-(time.monotonic()-started))))
    finally:
        for proc in processes:
            try:os.killpg(proc.pid,signal.SIGTERM)
            except ProcessLookupError:pass
        for f in files:f.close()
    events={}
    for role in ('receiving','sending'):
        f=case_dir/(role+'.jsonl');events[role]=[json.loads(line) for line in f.read_text().splitlines()] if f.exists() else []
    probe={role:next((x for x in reversed(ev) if 'probeResult' in x),{}) for role,ev in events.items()}
    if case=='cancel':
        ok=probe['receiving'].get('probeResult')=='cancelled' and not (output/name).exists() and not list(output.glob('.*.part')) and probe['sending'].get('probeResult')=='error'
    elif case=='repeat':
        # Strict no-overwrite is an intentional failure, not a duplicate success.
        ok=probe['sending'].get('probeResult')=='error' and hashlib.sha256((output/name).read_bytes()).hexdigest()==before and not list(output.glob('.*.part'))
    else:
        received=output/name
        ok=codes==[0,0] and all(x.get('probeResult')=='done' for x in probe.values()) and received.exists() and hashlib.sha256(received.read_bytes()).hexdigest()==digest and not list(output.glob('.*.part'))
    teardown_errors={role:[x for x in ev if
        (x.get('event',{}).get('kind')=='path-diagnostic' and x['event']['summary']['phase']=='connection_failed') or
        (x.get('event',{}).get('kind')=='connection-diagnostic' and x['event']['summary']['stage']=='attempt_abort')]
        for role,ev in events.items()}
    if case not in ('repeat','cancel'):
        ok=ok and all(not errors for errors in teardown_errors.values())
    identity_ok=all(x.get('version')=='0.6.38' and x.get('buildIdentity')==a.expected_build_id for x in probe.values())
    route_evidence={role:[x['event']['summary'] for x in ev if x.get('event',{}).get('kind')=='path-diagnostic' and x['event']['summary']['phase']=='selected_pair'] for role,ev in events.items()}
    if case=='turn':
        ok=ok and all(any(d['route']=='turn_relay' and d['nominated'] is True and d['pair_state']=='succeeded' and d['local_type']=='relay' and d['remote_type']=='relay' for d in ds) for ds in route_evidence.values())
    def phase(role,name):
        return next((x['probeElapsedMs'] for x in events[role] if x.get('event',{}).get('kind')=='path-diagnostic' and x['event']['summary']['phase']==name),None)
    def stage(role,name):
        return next((x['probeElapsedMs'] for x in events[role] if x.get('event',{}).get('kind')=='native-task' and x['event']['status']['stage']==name),None)
    def delta(end,start):return None if end is None or start is None else end-start
    s_first=phase('sending','first_real_bytes');s_last=stage('sending','finalizing');s_ack=phase('sending','durable_ack_received')
    r_last=stage('receiving','finalizing');r_commit=phase('receiving','atomic_commit');r_ack=phase('receiving','durable_ack_sent')
    timings={'senderDataWindowMs':delta(s_last,s_first),'senderLastFrameToDurableAckMs':delta(s_ack,s_last),'receiverSaveAndFinalAckMs':delta(r_ack,r_last),'receiverFinalizeToAtomicCommitMs':delta(r_commit,r_last),'receiverFinalAckPublishMs':delta(r_ack,r_commit),'senderTaskToDurableAckMs':delta(s_ack,stage('sending','preparing'))}
    result={'case':case,'bytes':size,'sha256':digest,'sameHost':True,'processExitCodes':codes,'probe':probe,'identityPass':identity_ok,'routeEvidence':route_evidence,'teardownErrors':teardown_errors,'timings':timings,'pass':ok and identity_ok,'elapsedMs':round((time.monotonic()-started)*1000,3)}
    results.append(result);print(json.dumps(result,ensure_ascii=False),flush=True)
    (root/'summary.json').write_text(json.dumps(results,ensure_ascii=False,indent=2)+'\n')
    if not result['pass']:raise SystemExit(1)
print('ALL ACTUAL LINUX APP CASES PASSED (same host, not Windows/WAN)',flush=True)
