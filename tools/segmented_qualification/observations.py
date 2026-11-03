"""Domain observation vectors; no monitor deployment or service actuation."""
from __future__ import annotations
from pathlib import Path
from storage import atomic


def evaluate(fact,now):
    if now-fact['observed_at']>fact['max_age']:
        return [{'id':'labelwatch.storage.input_stale','state':'UNKNOWN','message':'Labelwatch storage evidence is stale; retention/archive health is unknown, not recovered.'}]
    out=[]
    for segment in fact['segments']:
        identity=segment['identity'];state=segment['state']
        if state=='SEALED':
            out.append({'id':'labelwatch.segment.sealed_pending_archive','segment':identity,'state':'FAILED' if now>segment['archive_deadline'] else 'PENDING','message':f'Labelwatch segment {identity} is sealed but not archived; local retirement is blocked.','cause':segment.get('blocked_reason')})
        elif state=='ARCHIVED':out.append({'id':'labelwatch.archive.custody_established','segment':identity,'state':'VERIFIED','message':f'Labelwatch segment {identity} has verified archive custody; retirement is eligible after reader lease and coverage checks.'})
        elif state=='RETIRED':out.append({'id':'labelwatch.retirement.complete','segment':identity,'state':'CLEAR','message':f'Labelwatch segment {identity} was retired with verified archive custody.'})
    return out


def qualify(destination):
    fact={'observed_at':100,'max_age':30,'segments':[{'identity':'2026-09-28','state':'SEALED','archive_deadline':90,'blocked_reason':'archive_capacity_unavailable'}]}
    failed=evaluate(fact,100);assert failed[0]['state']=='FAILED' and 'not archived' in failed[0]['message']
    unknown=evaluate(fact,131);assert unknown[0]['state']=='UNKNOWN'
    fact['observed_at']=132;fact['segments'][0]['state']='ARCHIVED';recovered=evaluate(fact,132);assert recovered[0]['state']=='VERIFIED'
    fact['segments'][0]['state']='RETIRED';retired=evaluate(fact,133);assert retired[0]['state']=='CLEAR'
    # Immutable history includes failed and later superseding observations.
    history=[{'at':100,'conditions':failed},{'at':131,'conditions':unknown},{'at':132,'conditions':recovered,'supersedes_at':100},{'at':133,'conditions':retired}]
    atomic(destination,{'result':'PASS','history':history,'operator_surface':'Qualification JSON and documented operator text only; no live Constellation enrollment/notification claim','authority':'Observation only; no collector/retention/release actuation'})
