import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from lot_lineage.pipeline import build
FIX=Path(__file__).resolve().parents[1]/'fixtures/lots.json'
class LineageTest(unittest.TestCase):
    def setUp(self):
        t=tempfile.TemporaryDirectory();self.addCleanup(t.cleanup);self.root=Path(t.name);self.db=self.root/'out.sqlite';self.data=json.loads(FIX.read_text())
    def modified(self):
        p=self.root/'input.json';p.write_text(json.dumps(self.data));return p
    def test_exact_multilevel_and_conservation(self):
        r=build(FIX,self.db)
        self.assertEqual((r['raw_input_g'],r['process_loss_g'],r['available_g']),(1800,100,1700))
        y=next(x for x in r['lots'] if x['lot_id']=='Y')
        self.assertEqual(y['origin_fraction'],{'A':'9/25','B':'6/25','C':'2/5'})
        self.assertEqual(r['available_origin_g'],{'A':'940','B':'460','C':'300'})
        with sqlite3.connect(self.db) as d:
            self.assertEqual(d.execute('PRAGMA foreign_key_check').fetchall(),[])
            self.assertEqual(d.execute("SELECT numerator,denominator FROM origin_mix WHERE lot_id='Y' AND origin_lot='A'").fetchone(),('9','25'))
    def test_repeat_and_recipe_order(self):
        expected=build(FIX,self.db);self.data['recipes'].reverse()
        self.assertEqual(build(self.modified(),self.db),expected)
    def test_mass_mismatch(self):
        self.data['recipes'][0]['loss_g']=1
        with self.assertRaisesRegex(ValueError,'not conserved'):build(self.modified(),self.db)
    def test_overconsumption(self):
        self.data['lots'][0]['mass_g']=500
        with self.assertRaisesRegex(ValueError,'over-consumed'):build(self.modified(),self.db)
    def test_cycle(self):
        self.data={'lots':[{'lot_id':'X','kind':'produced','mass_g':10},{'lot_id':'Y','kind':'produced','mass_g':10}],
         'recipes':[{'output_lot':'X','inputs':[{'lot_id':'Y','mass_g':10}],'loss_g':0},{'output_lot':'Y','inputs':[{'lot_id':'X','mass_g':10}],'loss_g':0}]}
        with self.assertRaisesRegex(ValueError,'cyclic'):build(self.modified(),self.db)
    def test_missing_recipe(self):
        self.data['recipes'].pop()
        with self.assertRaisesRegex(ValueError,'missing recipe'):build(self.modified(),self.db)
    def test_boolean_mass(self):
        self.data['lots'][0]['mass_g']=True
        with self.assertRaises(ValueError):build(self.modified(),self.db)
    def test_failure_preserves_target(self):
        build(FIX,self.db);before=self.db.read_bytes();self.data['recipes'][0]['inputs'][0]['lot_id']='missing'
        with self.assertRaises(ValueError):build(self.modified(),self.db)
        self.assertEqual(self.db.read_bytes(),before)
    def test_duplicate_parent(self):
        self.data['recipes'][0]['inputs'].append(self.data['recipes'][0]['inputs'][0].copy())
        with self.assertRaises(ValueError):build(self.modified(),self.db)
