import argparse
import json
import sqlite3
from .pipeline import build

def main():
    p=argparse.ArgumentParser(description='Validate lot genealogy and materialize exact origin fractions')
    p.add_argument('--source',required=True);p.add_argument('--database',required=True)
    a=p.parse_args()
    try:r=build(a.source,a.database)
    except (ValueError,OSError,sqlite3.Error) as e:p.exit(2,str(e)+'\n')
    print(json.dumps(r,sort_keys=True))
if __name__=='__main__':main()
