
import pgc

pg = pgc.pgc(('127.0.0.1', 5432))
pg.init([('user', 'postgres'),
         ('database', 'test'),
         ('application_name', 'dodo')])


rows = pg.query("insert into abc (name, i) values('sss', 10);")
print(rows)

rows = pg.query('select * from abc;')
print(rows)

rows = pg.query("delete from abc where name='sss';")
print(rows)


