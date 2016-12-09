#!/usr/bin/python 
from org.apache.pig.scripting import *
P = Pig.compile("""
previous_pagerank = LOAD '$docs_in' USING PigStorage('\t') AS ( url: chararray, pagerank: float, links:{ link: ( url: chararray ) } );
outbound_pagerank = FOREACH previous_pagerank GENERATE pagerank / COUNT ( links ) AS pagerank, FLATTEN ( links ) AS to_url; 
new_pagerank = FOREACH ( COGROUP outbound_pagerank BY to_url, previous_pagerank BY url INNER ) GENERATE group AS url, ( 1 - $d ) + $d * SUM ( outbound_pagerank.pagerank ) AS pagerank, FLATTEN ( previous_pagerank.links ) AS links;
ordered = ORDER new_pagerank BY pagerank DESC;
STORE ordered INTO '$docs_out' USING PigStorage('\t');""")

# 'd' tangling value in pagerank model
params = { 'd': '0.85', 'docs_in': "datas/etl-xaaCC-MAIN.txt" }

for i in range(5):
	output = "output/pagerank_data_" + str(i + 1)
	params["docs_out"] = output
	stats = P.bind(params).runSingle()
	if not stats.isSuccessful():
		raise 'failed'
	params["docs_in"] = output
 
