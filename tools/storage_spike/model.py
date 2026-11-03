"""Bounded archive-first reachability model; stated assumptions matter."""
import json
from collections import deque

def check():
    # source, complete_archive, verified, durable_receipt, catalog
    initial=(True,False,False,False,False);seen={initial};q=deque([initial]);transitions=0
    while q:
        s,a,v,r,c=q.popleft()
        successors=[(s,a,v,r,c)] # process loss preserves durable objects
        if s:successors.append((s,True,v,r,c))
        if a:successors.append((s,a,True,r,c))
        if v:successors.append((s,a,v,True,c))
        if a and v and r:successors.append((False,a,v,r,c))
        if not s and r:successors.append((s,a,v,r,True))
        for state in successors:
            transitions+=1
            assert state[0] or (state[1] and state[2] and state[3]),state
            if state not in seen:seen.add(state);q.append(state)
    return {'proposition':'source retirement implies complete independently verified archive and durable receipt',
            'states':len(seen),'transitions':transitions,'result':'PASS',
            'assumptions':['accepted set fixed before sealing','archive stable after verification','atomic durable publication primitives behave as specified','no concurrent writer','no correlated custody loss'],
            'excluded':['power failure/NFS server semantics','cursor/state atomicity','schema migrations','late arrivals','provider loss after retirement'],'owner':'Codex /root; design evidence, not production acceptance'}

if __name__=='__main__':print(json.dumps(check(),indent=2))
