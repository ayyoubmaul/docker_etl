from __future__ import absolute_import
import argparse
import logging
import re
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import os

CWD = os.path.dirname(__file__)
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.path.join(CWD, "service_account.json")

class DataIngestion:
    def parse_method(self, string_input):
        values = re.split(",",
                          re.sub('\r\n', '', re.sub(u'"', '', string_input)))
        row = dict(
            zip(('searchTerms', 'rank', 'title', 'snippet', 'displayLink', 'link', 'queryTime', 
                'totalResults', 'cacheId', 'formattedUrl', 'htmlFormattedUrl', 'htmlSnippet', 'htmlTitle', 
                'kind', 'pagemap', 'cseName', 'count', 'startIndex', 'inputEncoding', 'outputEncoding', 
                'safe', 'cx', 'gl', 'searchTime', 'formattedSearchTime', 'formattedTotalResults'),
                values))
        return row


def run():
    """The main function which creates the pipeline and runs it."""

    # Parse arguments from the command line.
    data_ingestion = DataIngestion()

    p = beam.Pipeline()

    (
     p | 'Read from a File' >> beam.io.ReadFromText('gs://output/all_output.csv',
                                                  skip_header_lines=1)

     | 'String To BigQuery Row' >>
     beam.Map(lambda s: data_ingestion.parse_method(s))
     | 'Write to BigQuery' >> beam.io.Write(
         beam.io.BigQuerySink(
             'output.fly',
             schema='searchTerms:STRING,rank:INTEGER,title:STRING,snippet:STRING,displayLink:STRING,link:STRING,queryTime:TIMESTAMP,totalResults:INTEGER,cacheId:STRING,formattedUrl:STRING,htmlFormattedUrl:STRING,htmlSnippet:STRING,htmlTitle:STRING,kind:STRING,pagemap:STRING,cseName:STRING,count:INTEGER,startIndex:INTEGER,inputEncoding:STRING,outputEncoding:STRING,safe:STRING,cx:STRING,gl:STRING,searchTime:TIMESTAMP,formattedSearchTime:STRING,formattedTotalResults:STRING',
             create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
             write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)))
    p.run().wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
