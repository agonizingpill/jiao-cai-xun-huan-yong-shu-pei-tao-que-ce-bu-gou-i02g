import random
from sqlalchemy import select,func
from fastapi import HTTPException
from .models import RecheckBatch,RecheckItem,Copy,Assignment
from .common import inventory_lock,lock,pack,audit

def collect(s,b):
    # 并发下晚提交进已关批次的待批记录归当前批次；MySQL库存锁下不会出现，此处兜底
    for stray in s.scalars(select(RecheckItem).join(RecheckBatch,RecheckItem.batch_id==RecheckBatch.id).where(RecheckBatch.state=='closed',RecheckItem.disposition=='pending')):stray.batch_id=b.id
def current(s):
    b=s.scalar(select(RecheckBatch).where(RecheckBatch.state=='open').with_for_update())
    if not b:
        n=s.scalar(select(func.count()).select_from(RecheckBatch))or 0
        b=RecheckBatch(code='RB-'+str(n+1).zfill(3));s.add(b);s.flush()
    collect(s,b);return b
def hold(s,copy,assignment):
    copy.state='recheck';item=RecheckItem(batch_id=current(s).id,copy_id=copy.id,assignment_id=assignment.id);s.add(item);s.flush();return item
def turnover(s,copy_id):
    return s.scalar(select(func.count()).select_from(Assignment).where(Assignment.copy_id==copy_id,Assignment.state.in_(('issued','returned','lost'))))or 0
def estimate(s):
    # Only lottery-held copies give an unbiased sample; ranked holds would skew the rate.
    stats={}
    for i in s.scalars(select(RecheckItem).where(RecheckItem.sampled==True,RecheckItem.result.isnot(None))):
        edition=s.get(Copy,i.copy_id).edition_id;bad=1 if i.result=='repair'else 0
        for key in (('cell',edition,i.turnover),('edition',edition),('global',)):
            n,b=stats.get(key,(0,0));stats[key]=(n+1,b+bad)
    return stats
def probability(stats,edition,turnover):
    for key in (('cell',edition,turnover),('edition',edition),('global',)):
        n,b=stats.get(key,(0,0))
        if n:return b/n
    return None
def held_item(s,copy_id):
    return s.scalar(select(RecheckItem).where(RecheckItem.copy_id==copy_id,RecheckItem.disposition=='held',RecheckItem.result.is_(None)).order_by(RecheckItem.id.desc()))
def set_quota(s,id,d,u):
    b=lock(s,RecheckBatch,id)
    if b.state!='open':raise HTTPException(409,'批次已关，复检名额不能改')
    b.quota=d.quota;b.sample_quota=d.sample_quota;audit(s,u,'设定待检批次复检名额 '+b.code);return pack(b)
def summary(s,b,items):return pack(b)|{'held':[s.get(Copy,i.copy_id).barcode for i in items if i.disposition=='held'],'released':[s.get(Copy,i.copy_id).barcode for i in items if i.disposition=='released']}
def close(s,id,u):
    inventory_lock(s);b=lock(s,RecheckBatch,id)
    if b.state=='closed':
        items=list(s.scalars(select(RecheckItem).where(RecheckItem.batch_id==id).order_by(RecheckItem.id)))
        return summary(s,b,items)|{'note':'批次已关，重试不改留检与放行条码'}
    collect(s,b)
    items=list(s.scalars(select(RecheckItem).where(RecheckItem.batch_id==id).order_by(RecheckItem.id)))
    stats=estimate(s);cold=not stats.get(('global',),(0,0))[0]
    for i in items:
        copy=lock(s,Copy,i.copy_id);i.turnover=turnover(s,copy.id);i.probability=probability(stats,copy.edition_id,i.turnover)
    quota=min(b.quota,len(items));sample=quota if cold else min(b.sample_quota,quota)
    picked={x.id for x in random.Random(b.id).sample(items,sample)}if sample else set()
    ranked=sorted((i for i in items if i.id not in picked),key=lambda i:(-(i.probability or 0),i.id))[:quota-sample]
    chosen={i.id:False for i in ranked}|{x:True for x in picked};rank_of={i.id:n+1 for n,i in enumerate(ranked)}
    for i in items:
        copy=s.get(Copy,i.copy_id)
        if i.id in chosen:
            i.disposition='held';i.sampled=chosen[i.id]
            i.reason='抽签留检：本批随机样本，实查结果用于校准破损估计'if i.sampled else'概率留检：版次+周转估计破损率 %.2f，本批第 %d 高'%(i.probability or 0,rank_of[i.id])
        else:
            i.disposition='released';i.reason='放行：抽签未中，回可用架'if cold else'放行：估计破损率 %.2f 未入本批名额'%(i.probability or 0)
            if copy.state=='recheck':copy.state='available'
    b.state='closed';audit(s,u,'关批定留检与放行 '+b.code);return summary(s,b,items)
