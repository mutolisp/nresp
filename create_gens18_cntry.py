
import psycopg2, psycopg2.extras
import nresp
from progressbar import ProgressBar
import time

nc = nresp.nresp()
nc.__init__()
nc.conndb()

clist = nc.tab_attrlist('world_adm0_noantarct', 'gmi_cntry')
p = ProgressBar('green', width=50, block='#', empty=' ')

for i in xrange(len(clist)):
    prog= int(i*100.0/len(clist))
    p.render(prog, 'Step %i, %s\n' % ( i, clist[i] ))
    gmi = clist[i].lower()
    # curs = nc.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    # drop = """ DROP TABLE IF EXISTS world_adm0_%s;""" % gmi
    # curs.execute(drop)
    # nc.conn.commit()
    # query = """ CREATE TABLE world_adm0_%s AS (
    # SELECT * from world_adm0_noantarct WHERE gmi_cntry = '%s'
    # )
    # """ % ( gmi, clist[i] )
    # curs.execute(query)
    # nc.conn.commit()
    # cindex = """ CREATE INDEX idx_world_adm0_%s_geom ON world_adm0_%s
    # USING gist (the_geom) """ % ( gmi, gmi )
    # curs.execute(cindex)
    # nc.conn.commit()
    # analyze = " ANALYZE world_adm0_%s" % gmi
    # curs.execute(analyze)
    # nc.conn.commit()

    ctab = 'world_adm0_%s' % gmi
    nc.intst_geom_cntrytab(cntry_tab=ctab, cntry=clist[i], \
        cntry_col='gmi_cntry', geo_tab='gens_v2_valid', geo_col='genzv2_seq')
