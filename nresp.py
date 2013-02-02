#!/usr/bin/env python
"""
/***************************************************************************
 nresp.py 
 National responsibility assessment tool library
                              -------------------
        begin                : 2012-12-25
        copyright            : (C) 2012 by Lin, Cheng-Tao
        email                : mutolisp@gmail.com
 ***************************************************************************/

/***************************************************************************
 *                                                                         *
 *   This program is free software; you can redistribute it and/or modify  *
 *   it under the terms of the GNU General Public License as published by  *
 *   the Free Software Foundation; either version 2 of the License, or     *
 *   (at your option) any later version.                                   *
 *                                                                         *
 ***************************************************************************/
"""


import psycopg2, psycopg2.extras
from getpass import getpass
import glob, os, subprocess

class nresp():
    """
    National responsibility assessment tools for conservation biology
    nresp(db_host, db_name, db_user) to establish PostgreSQL database connections
    """
    def __init__(self):
        self.conn = None

    def conndb(self, db_host='140.112.82.15', db_name='nresp_db', db_user='postgres'):
        if self.conn == None:
            self.h = db_host
            self.n = db_name
            self.u = db_user
            passwd = 'postgres1119'
            conn_string = "host='%s' dbname='%s' user='%s' password='%s'" % (db_host, db_name, db_user, passwd)
            try:
                conn = psycopg2.connect(conn_string)
                self.conn = conn
                print("Connection established!")
            except Exception, e:
                pass
                print(e)
    
    def import_shp2psql(self, fname):
        """
        Import shp file into postgresql database with 'shp2pgsql'. This function
        will backup original (if exists) table to OrigTableName_bak
        Usage: 
                import_shp2pgsql(fname)
                    fname is the path of given shpfile
        Example:
        >>> import_shp2pgsql('/tmp/grid.shp')

        """
        try:
            if self.conn is None:
                print("Please connect db with nresp(dbhost, dbname, dbuser) first!")
            else:
                curs = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
                f = os.path.basename(fname).split(".")[0]
                sql_drop_tab = "DROP TABLE IF EXISTS %s_bak;" % f
                curs.execute(sql_drop_tab)
                self.conn.commit()
                sql_backup_tab = "CREATE TABLE %s_bak AS (SELECT * FROM %s);" % ( f, f )
                curs.execute(sql_backup_tab)
                self.conn.commit()
                shp2pgsql_cmd = ["shp2pgsql -d -D -s 4326 -I -g the_geom " + fname + \
                    " | psql -h" + self.h + " -U " + self.u + " -d " + self.n  ]
                subprocess.call(shp2pgsql_cmd, shell=True)
                print("Importing %s.shp finished") % f
        except Exception, e3:
            pass
            print(e3)


    def create_ocntry_tab(self, cntry):
        """
        This function will create a table named with o_cntry to store output values.
        The output table schema is:
        o_cntry(binomial character varying, iucn_status character(2), g_num integer, 
        global_distr integer, sp_area double precision, dpexp double precision, dpobs 
        double precision, resp double precision, resp_val integer, resp_class integer
        Note: create_ocntry_tab will destroy existing table!

        Example:
        >>> create_ocntry_tab('taiwan')

        """
        try:
            if self.conn is None:
                print("Please connect db with nresp(dbhost, dbname, dbuser) first!")
            else:
                curs = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
                sql_drop_tab = "DROP TABLE IF EXISTS o_%s" % cntry
                curs.execute(sql_drop_tab)
                self.conn.commit()
                sql_create_tab = """ 
                CREATE TABLE o_%s (
                    binomial character varying,
                    iucn_status character(2),
                    g_num integer,
                    global_distr integer,
                    sp_area double precision,
                    dpexp double precision,
                    dpobs double precision,
                    resp double precision,
                    resp_val integer,
                    resp_class integer
                );
                """ % cntry
                curs.execute(sql_create_tab)
                self.conn.commit()
                print("Table o_%s created!")
        except Exception, ed1:
            pass
            self.conn.rollback()
            print(ed1)
        curs.close()

    def calc_area(self, ctab, col, crcol='', crval=''):
        """
        Calc_refarea will calculate the total area (square kilometers) of reference area
        calc_refarea(<reference area table>, <geometry column>, [criteria column], [criteria value])
        
        Example:
        >>> calc_refarea('asia_countries', 'the_geom')
        '44914379.5764'

        >>> calc_refarea('asia_countries', 'the_geom', 'cntry_name', 'Taiwan')
        '36021.0007'

        """
        try:
            if self.conn is None:
                print("Please connect db with nresp(dbhost, dbname, dbuser) first!")
            else:
                try:
                    curs = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
                    if crcol is None or crval is None:
                        sql_calc_refarea = """
                        SELECT sum(ST_Area(%s::geography)/1000000) 
                        FROM %s;
                        """ % ( col, ctab )
                    else:
                        sql_calc_refarea = """
                        SELECT sum(ST_Area(%s::geography)/1000000) 
                        FROM %s
                        WHERE %s = '%s';
                        """ % ( col, ctab, crcol, crval )
                    try:
                        curs.execute(sql_calc_refarea)
                        refarea_f = curs.fetchone()
                    except Exception, pe:
                        print(pe)
                    ref_area = float(format(refarea_f[0], '.4f'))
                    return(ref_area)
                except Exception, ed2:
                    pass
                    print(ed2)
        except Exception, ed21:
            pass
            self.conn.rollback()
            print(ed21)
        curs.close()

    def intst_area(self, a_tab, a, a_col, b_tab, b_col, geom_col='the_geom'):
        """
        intst_area will calculate intersecting area for given two polygons. It will return a
        dictionary list
        intst_area(<a table>, <a attribute>, <column name with a>, <b table>, 
            <b column>, [geometry column (default if 'the_geom')])

        Example:
        >>> nc.intst_area('all_amphibians_oct2012_s', 'Babina adenopleura',
                'binomial', 'asia_countries', 'cntry_name')
        
        """
        try:
            curs = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            sql_query = """SELECT * FROM 
            (SELECT w.%s, sum(ST_Area(ST_Intersection(sp.%s,w.%s)::geography)/1000000) sqkm 
                   FROM %s sp, %s w WHERE sp.%s = '%s' group by w.%s) as i 
            WHERE sqkm > 0;

            """ % ( b_col, geom_col, geom_col, a_tab, b_tab, a_col, a, b_col )
            curs.execute(sql_query)
            output = curs.fetchall()
            return(output)
        except Exception, pe:
            pass
            self.conn.rollback()
            print(pe)
        curs.close()

    def cal_dpexp(self, sptab, cntry_tab, sp, ref_area):
        curs = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        sql_dpexp = """UPDATE %s SET dpexp = 
                    (SELECT sum(ST_Area(ST_Intersection(sp.the_geom,w.the_geom)::geography)/1000000) sqkm 
                    FROM  %s sp, %s w WHERE binomial = '%s')/%s WHERE binomial = '%s'; """ % ( sptab,  sptab, cntry_tab, sp, ref_area, sp )
        curs.execute(sql_dpexp)
        self.conn.commit()
        curs.close()


    def calc_dpobs(self, cntry, cntry_tab, col):
        curs = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        sql_sp_dpobs = """UPDATE o_%s SET dpobs = sp_area/(SELECT sum(area_sqkm) FROM %s 
                WHERE %s='%s') WHERE sp_area > 0;
        """  % ( cntry, cntry_tab, col, cntry )
        curs.execute(sql_sp_dpobs)
        self.conn.commit()
        curs.close()

    def calc_resp(self, cntry):
        curs = self.conn.cursor()
        sql = "UPDATE o_%s SET resp = dpobs/dpexp;" % cntry
        curs.execute(sql)
        self.conn.commit()
        curs.close()

    def calc_global(self, cntry, col):
        curs = self.conn.cursor()
        sql_loc = "UPDATE o_%s SET global_distr = 0 WHERE %s = 1;" % (cntry, col)
        sql_reg = "UPDATE o_%s SET global_distr = 1 WHERE %s = 2 OR %s = 3;" % (cntry, col, col)
        sql_glob = "UPDATE o_%s SET global_distr = 2 WHERE %s > 3;" % (cntry, col)
        curs.execute(sql_loc)
        curs.execute(sql_reg)
        curs.execute(sql_glob)
        self.conn.commit()
        curs.close()

    def calc_resp_val(self, cntry, thres=2):
        curs = self.conn.cursor()
        sql_u = "UPDATE o_%s SET resp_val = 0 + global_distr where resp > %i " % ( cntry, thres )
        sql_l = "UPDATE o_%s SET resp_val = 1 + global_distr where resp <= %i " % ( cntry, thres )
        curs.execute(sql_u)
        curs.execute(sql_l)
        self.conn.commit()
        curs.close()


    def calc_resp_class(self, cntry):
        curs = self.conn.cursor()
        sql_find_class = """UPDATE o_%s SET resp_class = g.nresp_class
            FROM (select iucn_level, nresp_class, nresp_code from iucn_category) g
            WHERE g.iucn_level=o_%s.iucn_status and g.nresp_code = o_%s.resp_val
        """ % ( cntry, cntry, cntry )
        curs.execute(sql_find_class)
        self.conn.commit()
        curs.close()


    def update_cntry_col(self, sp, cntry, ucol, otab, ocol):
        curs = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        update_sql = """ update o_%s set 
        %s = (select distinct %s from 
        %s where binomial='%s') where binomial='%s';
        """ % (cntry, ucol, ocol, otab, sp, sp )
        curs.execute(update_sql)
        self.conn.commit()
        curs.close()

    def find_gensv2(self, sp):
        curs = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        gensv2_sql = """SELECT count(distinct g.genzv2_seq) 
        from all_amphibians_oct2012 s, gens_v2_s g 
        where s.binomial='%s' and st_intersects(s.the_geom, g.the_geom)
        """ % sp
        curs.execute(gensv2_sql)
        gensv2_num = curs.fetchone()
        g_num = int(gensv2_num[0])
        # update the 
        update_sql = """ UPDATE %s SET gensv2 = %s WHERE binomial = '%s' """ % ( sptab, g_num, sp )
        curs.execute(update_sql)
        self.conn.commit()
        prog = int((s+1)*1.0/len(splist)*100)
        print("%s, gensv2 number = %s (progress: %s)") % ( sp, g_num, prog )


    def eval_globdistr(self, sptab, biome_num_col):
        curs = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        sql_update_g1 = "UPDATE %s SET global_distr = 0 WHERE %s = 1;" % ( sptab, biome_num_col )
        sql_update_g2 = "UPDATE %s SET global_distr = 1 WHERE %s > 1 AND %s <=3;" % ( sptab, biome_num_col, biome_num_col )
        sql_update_g3 = "UPDATE %s SET global_distr = 2 WHERE %s > 3;" % ( sptab, biome_num_col )
        curs.execute(sql_update_g1)
        curs.execute(sql_update_g2)
        curs.execute(sql_update_g3)
        self.conn.commit()


