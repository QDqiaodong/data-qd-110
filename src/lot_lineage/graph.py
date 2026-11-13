from collections import defaultdict
from fractions import Fraction
import json
from pathlib import Path

def grams(value,zero=False):
    if type(value) is not int or value<(0 if zero else 1):raise ValueError('mass must be integer grams')
    if value>10**12:raise ValueError('mass exceeds supported limit')
    return value

def load(path):
    data=json.loads(Path(path).read_text(encoding='utf-8'))
    if not isinstance(data,dict) or set(data)!={'lots','recipes'}:raise ValueError('expected lots and recipes')
    if not isinstance(data['lots'],list) or not isinstance(data['recipes'],list):raise ValueError('expected arrays')
    lots={}
    for r in data['lots']:
        if not isinstance(r,dict) or set(r)!={'lot_id','kind','mass_g'}:raise ValueError('invalid lot fields')
        key=r['lot_id']
        if not isinstance(key,str) or not key.strip() or key in lots:raise ValueError('invalid or duplicate lot ID')
        if r['kind'] not in ('raw','produced'):raise ValueError('invalid lot kind')
        grams(r['mass_g']);lots[key]=r
    if not lots:raise ValueError('no lots')
    recipes={};consumed=defaultdict(int)
    for r in data['recipes']:
        if not isinstance(r,dict) or set(r)!={'output_lot','inputs','loss_g'}:raise ValueError('invalid recipe fields')
        key=r['output_lot']
        if not isinstance(key,str) or key not in lots or lots[key]['kind']!='produced' or key in recipes:raise ValueError('invalid or duplicate output recipe')
        grams(r['loss_g'],zero=True)
        if not isinstance(r['inputs'],list) or not r['inputs']:raise ValueError('recipe requires inputs')
        seen=set();total=0
        for i in r['inputs']:
            if not isinstance(i,dict) or set(i)!={'lot_id','mass_g'}:raise ValueError('invalid input fields')
            src=i['lot_id']
            if not isinstance(src,str) or src not in lots or src in seen:raise ValueError('unknown or duplicate input lot')
            seen.add(src);mass=grams(i['mass_g']);total+=mass;consumed[src]+=mass
        if total!=lots[key]['mass_g']+r['loss_g']:raise ValueError('recipe mass not conserved')
        recipes[key]=r
    if set(recipes)!={k for k,v in lots.items() if v['kind']=='produced'}:raise ValueError('produced lot missing recipe')
    for key,value in consumed.items():
        if value>lots[key]['mass_g']:raise ValueError('lot over-consumed: '+key)
    return lots,recipes,consumed

def propagate(lots,recipes):
    children=defaultdict(list);degree={k:0 for k in lots}
    for dest,r in recipes.items():
        for i in r['inputs']:children[i['lot_id']].append(dest);degree[dest]+=1
    queue=sorted(k for k,d in degree.items() if d==0);order=[];mix={}
    while queue:
        key=queue.pop(0);order.append(key)
        if lots[key]['kind']=='raw':mix[key]={key:Fraction(1)}
        else:
            recipe=recipes[key];total=sum(i['mass_g'] for i in recipe['inputs']);origins=defaultdict(Fraction)
            for i in recipe['inputs']:
                for origin,f in mix[i['lot_id']].items():origins[origin]+=f*Fraction(i['mass_g'],total)
            mix[key]=dict(origins)
        if sum(mix[key].values())!=1:raise ValueError('origin fractions must sum to one')
        for dest in sorted(children[key]):
            degree[dest]-=1
            if degree[dest]==0:queue.append(dest);queue.sort()
    if len(order)!=len(lots):raise ValueError('cyclic genealogy')
    return mix,order
