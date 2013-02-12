:mod:`nresp` National responsibility and biodiversity assessment tools
======================================================================

The ``nresp`` class
===================

.. class:: nresp

   :synopsis: general nresp library


.. py:function:: __init__()

   initialize the nresp()


.. py:function:: conndb()


   connecting the database
   
   Examples:

   >>> nc = nresp.nresp()
   >>> nc.__init__()
   >>> nc.conndb()


.. py:function:: calc_area(<table>, <geometry column>, [criteria column], [criteria value])


   calc_area will calculate the total area (square kilometers) of given criteria and return float number 
   (unit: square kilometers)


   Examples:

   >>> nc.calc_area('asia_countries', 'the_geom')
   44914379.5764
   
   >>> nc.calc_area('asia_countries', 'the_geom',  'cntry_name', 'Taiwan')
   36021.0007
 
   .. note:: 
      

      calc_area() use postgis ST_Area(geom::geography) to calculate the area, 
      thus the geometry column of given table should be valid WGS 1984 (EPSG:4326) coordinate system 


.. py:function:: cal_dpexp(<sp>, <sp_tab>, <sp_col>, <geo_tab>, <ref_area>, [sp_geom_col='the_geom'], [geo_geom_col='the_geom'])
   

   Calculate expected distribution patterns (dpexp).
   'dpexp' is defined as distribution in reference area / total reference area.

   The distribution in reference area is intersecting areas of <sp> 
   (in table <sptab> of column <sp_col>) and <geo_tab>. <ref_area> is 
   the total area of given reference area (floating number).

   Examples:

   >>> nc.cal_dpexp('Babina adenopleura', 'all_amphibians_oct2012_s', 'binomial', 'gensv2_s')

.. py:function:: calc_dpobs(<cntry>, <foc_area>, [cntry_tab_prefix='o'])

   
   Calculate observed distribution patterns (dpobs). dpobs = distribution in focal area / total focal area

   Parameters:
   <cntry> is the target table name to store dpobs values
   (default table name is o_<cntry>) ; <foc_area> is the total focal area (floating number).


   Examples:

   >>> calc_dpobs('taiwan', '20101.35')

.. py:function:: calc_global(<cntry>, <col>, [crlower=1], [crupper=3], [cntry_tab_prefix='o'])

   
   :function::calc_global() will calculate the global distribution value (0: local; 1: regional; 2: global/wide distribution) and update values to table <cntry>.

   <cntry> is the target table name, and <col> is the column name of intersecting numbers of bioecoregion (ex: biomes). [crlower] is the threshold of lower value (integer; default is 1) to determine the global distribution patterns, while [crupper] is the threshold of upper value (integer; default is 3). If the values in <col> is [crlower], it indicates the species distribution pattern is local (column <global_distr> is set to 0); if the values in <col> is greater than [crlower] and less and equal than [crupper], the distribution pattern is regional (<global_distr> set to 1); if the values in <col> is greater than [crupper], the distribution pattern is wide/global.


   Examples:

   >>> nc.calc_global('taiwan', 'biome')


.. py:function:: calc_resp(<cntry>, [cntry_tab_prefix='o'])

   
   calc_resp_val will calculate national responsibility values according to         dpobs/dpexp (i.e. resp column in output tables) thresholds. The default threshold is 2

   Examples:

   >>>  nc.calc_resp_val('taiwan')

.. py:function:: calc_resp_val(<cntry>, [thres=2], [cntry_tab_prefix='o'])


   calc_resp_val will calculate national responsibility values according to         dpobs/dpexp (i.e. resp column in output tables) thresholds. The default threshold is 2

   Examples:

   >>>  nc.calc_resp_val('taiwan')

.. py:function:: calc_resp_class(<cntry>, [cntry_tab_prefix='o'])


   Calculate the national responsibility class (require iucn_category table)

   Examples:

   >>> calc_resp_class('taiwan')
   
.. py:function:: create_ocntry_tab(<cntry>, [prefix='o'])  
   

   This function will create a table named with prefix_cntry to store output values (default prefix is 'o')   
   
   The output table schema is:
   o_cntry(binomial character varying, iucn_status character(2), g_num integer, global_distr integer, sp_area double precision, dpexp double precision, dpobs double precision, resp double precision, resp_val integer, resp_class integer


   Examples:

   >>> nc.create_ocntry_tab('taiwan')

   .. note::

   
      create_ocntry_tab will destroy existing table!



.. py:function:: find_gensv2(<sp>)


.. py:function:: intst_area(<a_tab>, <a>, <a_col>, <b_tab>, <b_col>, [geom_acol='the_geom'], [geom_bcol='the_geom'])

   
   intst_area() will find the intersecting area of given two polygons (or two multipolygons).
   <a_tab> and <b_tab> are the the table names with a valid polygon geometry column (default is 'the_geom'). <a> is target attribute in <a_col>, while <b_col> is the target attribute to intersect with <a> in <a_col>.

   :param <a_tab>: table a, which contains species data
   :param <a>: instance a, ex: 'Babina adenopleura'
   :param <a_col>: column name contains <a>, ex: species
   :param <b_tab>: table b, which is geographical layers, such as country
   :param <b_col>: column name of <b_tab>, ex: 'name' (of countries)
   :param [geom_acol]: geometry column of table <a_tab>
   :param [geom_acol]: geometry column of table <b_tab>

   Examples:

   >>> nc.intst_area('all_amphibians_oct2012_s', 'Babina adenopleura', 'binomial', 'asia_countries', 'cntry_name') 


.. py:function:: intst_geom_cntrytab(<cntry_tab>, <cntry>, <cntry_col>, <geo_tab>, <geo_col>, [cntry_geom='the_geom'], [geo_geom='the_geom'])

  
   intersecting country and bioecozones, then calculate the area of given bioecozones in specific country

   Examples:
   

.. py:function:: intst_gnumlist(<a_tab>, <a_col>, <a>, <b_tab>, <b_col>, [geom_acol='the_geom'], [geom_bcol='the_geom'])


   Intersect <a_col>'s <a> record in <a_tab> and <b_tab>, then
   find the intersecting attributes of <b_col>


   Returns:

   dictionary list

   Examples:

   >>> nc.intst_attrlist('all_amphibians_oct2012_s', 'binomial', 'Babina adenopleura', \
           'gens_v2_valid', 'genzv2_seq')
   [[14], [10], [11], [18], [13], [5]]



.. py:function:: update_cntry_col(<sp>, <sp_col>, <cntry>, <ucol>, <otab>, <ocol>, [cntry_tab_prefix='o'])


   Update output table from another table for a specific species.


   Detail and parameters:

    This function will update output table (i.e. [cntry_tab_prefix]_<cntry>) column <ucol> from another table <otab>'s column <ocol> where the <sp> in <sp_col> is the same in output table


   Examples:

   >>> update_cntry_col('Babina adenopleura', 'binomial', 'taiwan', 'dpexp',
                         'all_amphibians_oct2012_s', 'dpexp')

   >>> update_cntry_col('Babina_adenopleura', 'binomial', 'taiwan', 'iucn_status',
                         'amphibian_high_taxnomy', 'iucn_level')


.. py:function:: tab_attrlist(<table>, <col>, [criteria=''])

   Export distinct attribute of a specific column from table

   Returns:

   list

   Examples:

   >>> tab_attrlist(table='world', col='cntry_name')


.. py:function:: intst_geom_cntrytab(self, cntry_tab, cntry, cntry_col, geo_tab, geo_col, cntry_geom='the_geom', geo_geom='the_geom')

   Examples:
