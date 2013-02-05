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
        Abstract:
        --------------------
        Import shp file into postgresql database with 'shp2pgsql'. This function
        will backup original (if exists) table to OrigTableName_bak
        Usage: 
                import_shp2pgsql(fname)
                    fname is the path of given shpfile
        Example:
        --------------------
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


    def create_ocntry_tab(self, cntry, prefix='o'):
        """
        Abstract:
        --------------------
        This function will create a table named with o_cntry to store output values.
        The output table schema is:
        o_cntry(binomial character varying, iucn_status character(2), g_num integer, 
        global_distr integer, sp_area double precision, dpexp double precision, dpobs 
        double precision, resp double precision, resp_val integer, resp_class integer
        Note: create_ocntry_tab will destroy existing table!

        Example:
        --------------------
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
                CREATE TABLE %s_%s (
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
                """ % ( prefix, cntry )
                curs.execute(sql_create_tab)
                self.conn.commit()
                print("Table %s_%s created!") % ( prefix, cntry)
        except Exception, ed1:
            pass
            self.conn.rollback()
            print(ed1)
        curs.close()

    def calc_area(self, ctab, col, crcol='', crval=''):
        """
        Abstract:
        --------------------
        Calc_refarea will calculate the total area (square kilometers) of reference area
        calc_refarea(<reference area table>, <geometry column>, [criteria column], [criteria value])
        
        
        Example:
        --------------------
        >>> calc_refarea('asia_countries', 'the_geom')
        '44914379.5764'

        >>> calc_refarea('asia_countries', 'the_geom', 'cntry_name', 'Taiwan')
        '36021.0007'
        ----
        ReturnVal: float (precision: .4)

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

    def intst_area(self, a_tab, a, a_col, b_tab, b_col, geom_acol='the_geom', geom_bcol='the_geom'):
        """
        Abstract:
        --------------------
        intst_area will calculate intersecting area for given two polygons. It will return a
        dictionary list
        intst_area(<a table>, <a attribute>, <column name with a>, <b table>, 
            <b column>, [geometry column (default if 'the_geom')])

        Example:
        --------------------
        >>> nc.intst_area('all_amphibians_oct2012_s', 'Babina adenopleura',
                'binomial', 'asia_countries', 'cntry_name')
        
        """
        try:
            curs = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            sql_query = """SELECT * FROM 
            (SELECT w.%s, sum(ST_Area(ST_Intersection(sp.%s,w.%s)::geography)/1000000) sqkm 
                   FROM %s sp, %s w WHERE sp.%s = '%s' group by w.%s) as i 
            WHERE sqkm > 0;

            """ % ( b_col, geom_acol, geom_bcol, a_tab, b_tab, a_col, a, b_col )
            curs.execute(sql_query)
            output = curs.fetchall()
            return(output)
        except Exception, pe:
            pass
            self.conn.rollback()
            print(pe)
        curs.close()

    def cal_dpexp(self, sp, sp_tab, sp_col, geo_tab, ref_area, sp_geom_col='the_geom', geo_geom_col='the_geom'):
        """
        Abstract:
        --------------------
        calculate expected distribution patterns (dpexp). 'dpexp' is defined as
        distribution in reference area / total reference area.
        The distribution in reference area is intersecting areas of <sp> 
        (in table <sptab> of column <sp_col>) and <geo_tab>. <ref_area> is the total
        area of given reference area (floating number),

        Example:
        --------------------
        >>> cal_dpexp('Babina adenopleura', 'all_amphibians_oct2012_s', 'binomial',
                  'gensv2_s')

        """
        try:
            curs = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            sql_dpexp = """UPDATE %s SET dpexp = 
                        (SELECT sum(ST_Area(ST_Intersection(sp.%s,w.%s)::geography)/1000000) sqkm 
                        FROM  %s sp, %s w WHERE %s = '%s')/%s WHERE %s = '%s'; 
            """ % ( sptab, sp_geom_col, geo_geom_col, sp_tab, geo_tab, sp_col, sp, ref_area, sp_col, sp )
            curs.execute(sql_dpexp)
            self.conn.commit()
        except Exception, pe:
            pass
            self.conn.rollback()
            print(pe)
        curs.close()


    def calc_dpobs(self, cntry, foc_area, cntry_tab_prefix='o'):
        """
        Abstract:
        --------------------
        Calculate observed distribution patterns (dpobs). dpobs = distribution in 
        focal area / total focal area

        Example:
        --------------------
        >>> calc_dpobs('taiwan', '20101.35')
        """
        try:
            curs = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            sql_sp_dpobs = """UPDATE %s_%s SET dpobs = sp_area/%s WHERE sp_area > 0;
            """  % ( cntry_tab_prefix, cntry, foc_area )
            curs.execute(sql_sp_dpobs)
            self.conn.commit()
        except Exception, pe:
            pass
            self.conn.rollback()
            print(pe)
        curs.close()

    def calc_resp(self, cntry, cntry_tab_prefix='o'):
        """
        Abstract:
        --------------------
        calc_resp will calculate national responsibility values (dpobs/dpexp)
        in table <cntry_tab_pref>_<cntry> and update the national responsibility values
        to column <resp>. 

        Example:
        --------------------
        >>> calc_resp('taiwan')

        """
        try:
            curs = self.conn.cursor()
            sql = "UPDATE %s_%s SET resp = dpobs/dpexp;" % ( cntry_tab_prefix, cntry )
            curs.execute(sql)
            self.conn.commit()
        except Exception, pe:
            pass
            self.conn.rollback()
            print(pe)
        curs.close()

    def calc_global(self, cntry, col, crlower=1, crupper=3, cntry_tab_prefix='o'):
        """
        Abstract:
        --------------------
        calc_global will calculate the global distribution 
        value (0: local; 1: regional; 2: global/wide distribution)
        < cntry_tab_prefix, cntry> is the target table name, and <col> is the column name of 
        intersecting numbers of bioecoregion (ex: biomes).
        [crlower] is the threshold of lower value (integer) to determine the 
        global distribution patterns, while [crupper] is
        the threshold of upper value (integer).
        If the values in <col> is [crlower]
        indicate the species distribution pattern is local (column <global_distr>
        will set to 0); if the values in <col> is greater than [crlower] and less and equal 
        than [crupper], the distribution pattern is regional (<global_distr> set to 1);
        if the values in <col> is greater than [crupper], the distribution pattern is wide/global

        Example:
        --------------------
        >>> calc_global('taiwan', 'biome')

        """
        try:
            curs = self.conn.cursor()
            sql_loc = """UPDATE %s_%s SET global_distr = 0 
                WHERE %s > 0 AND %s <= %s;""" % ( cntry_tab_prefix, cntry, col, col, crlower)
            sql_reg = """UPDATE %s_%s SET global_distr = 1 
                WHERE %s > %s AND %s <= %s;""" % ( cntry_tab_prefix, cntry, col, crlower, col, crupper)
            sql_glob = """UPDATE %s_%s SET global_distr = 2 
                WHERE %s > %s;""" % ( cntry_tab_prefix, cntry, col, crupper)
            curs.execute(sql_loc)
            curs.execute(sql_reg)
            curs.execute(sql_glob)
            self.conn.commit()
        except Exception, pe:
            pass
            self.conn.rollback()
            print(pe)
        curs.close()

    def calc_resp_val(self, cntry, thres=2, cntry_tab_prefix='o'):
        """
        Abstract:
        --------------------
        calc_resp_val will calculate national responsibility values according to 
        dpobs/dpexp (i.e. resp column in output tables) thresholds.
        The default threshold is 2

        Example:
        --------------------
        >>> calc_resp_val('taiwan')
        """
        try:
            curs = self.conn.cursor()
            sql_u = """UPDATE %s_%s SET resp_val = 0 + global_distr where resp > %i 
                """ % ( cntry_tab_prefix, cntry, thres )
            sql_l = """UPDATE %s_%s SET resp_val = 1 + global_distr where resp <= %i 
                """ % ( cntry_tab_prefix, cntry, thres )
            curs.execute(sql_u)
            curs.execute(sql_l)
            self.conn.commit()
        except Exception, pe:
            pass
            self.conn.rollback()
            print(pe)
        curs.close()


    def calc_resp_class(self, cntry, cntry_tab_prefix='o'):
        """
        Abstract:
        --------------------
        calculate the national responsibility class
        (require iucn_category table)

        Example:
        --------------------
        >>> calc_resp_class('taiwan')
        """
        try:
            curs = self.conn.cursor()
            ftab = '%s_%s' % ( cntry_tab_prefix, cntry ) 
            sql_find_class = """UPDATE %s SET resp_class = g.nresp_class
                FROM (select iucn_level, nresp_class, nresp_code from iucn_category) g
                WHERE g.iucn_level=%s.iucn_status and g.nresp_code = %s.resp_val
            """ % ( ftab, cntry, ftab, ftab )
            curs.execute(sql_find_class)
            self.conn.commit()
        except Exception, pe:
            pass
            self.conn.rollback()
            print(pe)
        curs.close()


    def update_cntry_col(self, sp, sp_col, cntry, ucol, otab, ocol, cntry_tab_prefix='o'):
        """
        Abstract:
        --------------------
        Update output table from another table for a specific species.

        Examples:
        --------------------
        update_cntry_col('Babina adenopleura', 'binomial', 'taiwan', 'dpexp',
                         'all_amphibians_oct2012_s', 'dpexp')

        update_cntry_col('Babina_adenopleura', 'binomial', 'taiwan', 'iucn_status',
                         'amphibian_high_taxnomy', 'iucn_level')
        """
        try:
            ftab = '%s_%s' % ( cntry_tab_prefix, cntry ) 
            curs = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            # update SQL:
            # update ftab set ucol = (select distinct ocol from otab where
            # sp_col='sp') where sp_col='sp';
            update_sql = """ update %s set 
            %s = (select distinct %s from 
            %s where %s='%s') where %s='%s';
            """ % ( ftab, ucol, ocol, otab, sp_col, sp, sp_col, sp )
            curs.execute(update_sql)
            self.conn.commit()
        except Exception, pe:
            pass
            self.conn.rollback()
            print(pe)
        curs.close()

    def find_sp_gnum(self, sp, sp_tab, sp_col, geo_tab, geo_col, \
            sp_geom_col='the_geom', geo_geom_col='the_geom'):
        """
        Abstract:
        --------------------
        find_sp_gnum will calculate the bioecozones number intesecting species range
        and bioecozones.

        Example:
        --------------------

        """
        try:
            curs = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            # select count(distinct g.geo_col)
            # from sp_tab s, geo_tab g
            # where s.sp_col='sp' and st_intersects(s.the_geom, g.the_geom)
            # ex: 
            gensv2_sql = """ SELECT i.%s, sum(i.area) (
            SELECT st_area(st_intersection(s.%s, g.%s)::geography) area, g.%s 
            from %s s, %s g 
            where s.%s='%s' and st_intersects(s.%s, g.%s)) as i group by i.%s, i.area;
            """ % ( geo_col, sp_geom_col, geo_geom_col, geo_col, \
                    sp_tab, geo_tab, sp_col, sp, sp_geom_col, geo_geom_col )
            curs.execute(gensv2_sql)
            gensv2_num = curs.fetchone()
            g_num = int(gensv2_num[0])
            # update the 
            update_sql = """ UPDATE %s SET gensv2 = %s WHERE binomial = '%s' """ % ( sptab, g_num, sp )
            curs.execute(update_sql)
            self.conn.commit()
            prog = int((s+1)*1.0/len(splist)*100)
            print("%s, gensv2 number = %s (progress: %s)") % ( sp, g_num, prog )
        except Exception, pe:
            pass
            self.conn.rollback()
            print(pe)
        curs.close()


    def intst_geom_cntrytab(self, cntry_tab, cntry, cntry_col, geo_tab, geo_col, cntry_geom='the_geom', geo_geom='the_geom'):
        """
        Abstract:
        --------------------



        Example: 
        --------------------
        >>> intst_geom_cntrytab(cntry_tab='asia_countries', cntry='TWN', \
                cntry_col='gmi_cntry', geo_tab='gens_v2_valid', geo_col='genzv2_seq')
        """
        try: 
            out_table = "%s_%s" % ( geo_tab, cntry )  
            curs = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            drop_sql = """ DROP TABLE IF EXISTS %s """ % out_table
            curs.execute(drop_sql)
            self.conn.commit()
            # select geom the_geom, gmi_cntry, st_area(geom::geography)/1000000 area, 
            # genzv2_seq from (select st_intersection(s.the_geom, g.the_geom) as geom, 
            # s.gmi_cntry, g.genzv2_seq  from world_adm0_afg s, gens_v2_valid g where 
            # st_intersects(s.the_geom, g.the_geom)) as a;
            create_sql = """CREATE TABLE %s AS (
            SELECT geom the_geom, st_area(geom::geography)/1000000 %s_area, %s FROM 
            (SELECT st_intersection(s.%s, g.%s) as geom, s.%s, g.%s FROM %s s,%s g
             WHERE s.%s='%s' AND st_intersects(s.%s, g.%s)) as a
            );
            """ % ( out_table, \
                    geo_tab, geo_col, \
                    cntry_geom, geo_geom, cntry_col, geo_col, cntry_tab, geo_tab, \
                    cntry_col, cntry, cntry_geom, geo_geom )
            curs.execute(create_sql)
            self.conn.commit()
            sql_add_gid = """alter table %s add column gid serial;
            """ % out_table
            sql_add_pk = """alter table %s add primary key (gid); 
            """ % out_table
            sql_create_gist = """create index idx_%s_geom on %s using gist (the_geom);
            """ % ( out_table, out_table ) 
            sql_analyze = "analyze %s;" % out_table
            curs.execute(sql_add_gid)
            self.conn.commit()
            curs.execute(sql_add_pk)
            self.conn.commit()
            curs.execute(sql_create_gist)
            curs.execute(sql_analyze)
            self.conn.commit()
        except Exception, pe:
            pass
            self.conn.rollback()
            print(pe)
        curs.close()

    def tab_attrlist(self, table, col):
        """
        Abstract
        --------
        Export distinct attribute of a specific column from table

        Examples
        --------
        tab_attrlist(table='world', col='cntry_name')

        Returns
        -------
        list
        """
        try:
            curs = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            query = """ SELECT distinct %s FROM %s WHERE %s IS NOT NULL ORDER BY %s;
            """ % ( col, table, col,  col )
            curs.execute(query)
            result = curs.fetchall()
            rlist = []
            for i in xrange(len(result)):
                rlist.append(result[i][0])
            return(rlist)
        except Exception, pe:
            pass
            self.conn.rollback()
            print(pe)
        curs.close()

        # create table gensv2_taiwan as (select geom the_geom, cntry_name, st_area(geom::geography) gensv2_area from (select distinct st_intersection(s.the_geom, g.the_geom) as geom, s.cntry_name from asia_countries s, gens_v2_valid g where s.cntry_name='Taiwan' and st_intersects(s.the_geom, g.the_geom)) as a);
        # alter table gensv2_taiwan add column gid serial;
        # alter table gensv2_taiwan add primary key (gid);
        # create index idx_gensv2_taiwan_geom on gensv2_taiwan using gist (the_geom);
        # analyze gensv2_taiwan ;


