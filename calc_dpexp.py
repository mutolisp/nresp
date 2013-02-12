#!/usr/bin/env python

import nresp
import psycopg2, psycopg2.extras

nc = nresp.nresp()
nc.__init__()
nc.conndb()
curs = nc.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

### 0. PREPROCESS ####
# 
# define variables
species_tab = 'all_amphibians_oct2012_s'
species_col = 'binomial' 
country_tab = 'world_adm0_noantarct' 
ecozone_tab = 'gens_v2_valid'
region_tab = 'world_adm0_asia'
iucnlvl_tab = 'iucn_category'
taxonomy_tab = 'amphibian_higher_taxonomy'
outpref = 'oa'


# target countries: asia
# 0.0 obtain country list 
clist = nc.tab_attrlist(country_tab, 'gmi_cntry', "region='Asia'")
cclist = []
for i in xrange(len(clist)):
    if clist[i] is not '':
        cclist.append(clist[i])

# 0.1 create output tables (country-based)
for i in xrange(len(cclist)):
    nc.create_ocntry_tab(cclist[i], outpref)

# 0.2 calculate geo-econzon basic information:
# a) reference area: get list each attribute (genzv2_seq)
# get distinct genzv2_seq records of gens_v2_valid
glist = nc.tab_attrlist(ecozone_tab, 'genzv2_seq')

# b) calculate area of each genzv2_seq record (18 strata)
ga={}
for i in xrange(len(glist)):
    cr = 'genzv2_seq'
    crvalue = glist[i]
    rv = nc.calc_area(ecozone_tab, 'the_geom', cr, crvalue)
    ga.update({rv[0]:rv[1]})

# 0.3 get country areas
# note: column 'area_sqkm' needs to be calculated first 
# get 
get_area_sql = """select gmi_cntry, sum(area_sqkm) 
from %s
where region='Asia' and gmi_cntry ~ '[A-Z]' 
group by gmi_cntry
order by gmi_cntry;
""" % country_tab

curs.execute(get_area_sql)
cntry_area = dict(curs.fetchall())


### 1. Calculate species level information
#
# 1.0 Get species list 
slist = nc.tab_attrlist(species_tab, species_col, 'dpexp=0')


### 1.1 Calculate DPEXP of each species ####
# calculate dpexp= {species area in terrestrial region}/(reference area)
# reference area is defined as the total geo-ecozone area contains the
# target species
# calculate the geo-ecozones intersected by species range
for s in xrange(len(slist)):
    sp_ilist = nc.intst_gnumlist(species_tab, species_col, slist[s], ecozone_tab, 'genzv2_seq')
    # geo-ecozones number
    gnum = len(sp_ilist)
    refarea = 0
    for i in xrange(len(sp_ilist)):
        refarea = refarea + ga[sp_ilist[i][0]]
    # update dpexp to species table
    nc.calc_dpexp(slist[s], species_tab, species_col, ecozone_tab, refarea)


