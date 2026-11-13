from contextlib import closing
from collections import defaultdict
from fractions import Fraction
import os
from pathlib import Path
import sqlite3
import tempfile
from .graph import load,propagate

def build(source,target):
    lots,recipes,consumed=load(source);mix,order=propagate(lots,recipes)
    available_origins=defaultdict(Fraction);result=[]
    for key in sorted(lots):
        left=lots[key]['mass_g']-consumed[key]
        for origin,fraction in mix[key].items():available_origins[origin]+=left*fraction
        result.append({'lot_id':key,'mass_g':lots[key]['mass_g'],'available_g':left,
                       'origin_fraction':{o:str(f) for o,f in sorted(mix[key].items())}})
    raw_total=sum(r['mass_g'] for r in lots.values() if r['kind']=='raw')
    loss=sum(r['loss_g'] for r in recipes.values());available=sum(r['available_g'] for r in result)
    if raw_total!=loss+available:raise ValueError('global mass not conserved')
    target=Path(target);target.parent.mkdir(parents=True,exist_ok=True)
    fd,temp=tempfile.mkstemp(prefix='.lineage-',suffix='.tmp',dir=target.parent);os.close(fd)
    try:
        with closing(sqlite3.connect(temp)) as db, db:
            db.execute('PRAGMA foreign_keys=ON')
            db.executescript('''
            CREATE TABLE lots(lot_id TEXT PRIMARY KEY,kind TEXT NOT NULL,mass_g INTEGER NOT NULL,available_g INTEGER NOT NULL);
            CREATE TABLE transforms(output_lot TEXT PRIMARY KEY REFERENCES lots(lot_id),loss_g INTEGER NOT NULL);
            CREATE TABLE inputs(output_lot TEXT REFERENCES transforms(output_lot),input_lot TEXT REFERENCES lots(lot_id),mass_g INTEGER NOT NULL,PRIMARY KEY(output_lot,input_lot));
            CREATE TABLE origin_mix(lot_id TEXT REFERENCES lots(lot_id),origin_lot TEXT REFERENCES lots(lot_id),numerator TEXT NOT NULL,denominator TEXT NOT NULL,PRIMARY KEY(lot_id,origin_lot));
            CREATE VIEW available_origin_estimate AS SELECT origin_lot,sum(l.available_g*CAST(m.numerator AS REAL)/CAST(m.denominator AS REAL)) AS available_g
              FROM lots l JOIN origin_mix m USING(lot_id) GROUP BY origin_lot;
            ''')
            db.executemany('INSERT INTO lots VALUES (?,?,?,?)',[(r['lot_id'],lots[r['lot_id']]['kind'],r['mass_g'],r['available_g']) for r in result])
            for key in sorted(recipes):
                recipe=recipes[key];db.execute('INSERT INTO transforms VALUES (?,?)',(key,recipe['loss_g']))
                db.executemany('INSERT INTO inputs VALUES (?,?,?)',[(key,i['lot_id'],i['mass_g']) for i in recipe['inputs']])
            for key in sorted(mix):
                for origin,f in sorted(mix[key].items()):db.execute('INSERT INTO origin_mix VALUES (?,?,?,?)',(key,origin,str(f.numerator),str(f.denominator)))
        os.replace(temp,target)
    finally:Path(temp).unlink(missing_ok=True)
    return {'raw_input_g':raw_total,'process_loss_g':loss,'available_g':available,'topological_order':order,'lots':result,'available_origin_g':{k:str(v) for k,v in sorted(available_origins.items())}}
