import apache_beam as beam
from apache_beam.options import pipeline_options
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.runners import DataflowRunner

import google.auth
from datetime import datetime, timedelta
import json
from apache_beam.io.gcp.internal.clients import bigquery



# Setting up the Apache Beam pipeline options.
options = pipeline_options.PipelineOptions(flags=['--streaming'])

options.view_as(pipeline_options.StandardOptions).streaming = True
_, options.view_as(GoogleCloudOptions).project = google.auth.default()

options.view_as(GoogleCloudOptions).region = 'us-west1'
options.view_as(GoogleCloudOptions).staging_location = 'gs://abdul-dataflow-final/staging'
options.view_as(GoogleCloudOptions).temp_location = 'gs://abdul-dataflow-final/temp'

#options.view_as(pipeline_options.SetupOptions).sdk_location = (
#            f'/root/apache-beam-custom/packages/beam/sdks/python/dist/apache-beam-{beam.version.__version__}0.tar.gz' )


topic_stream = "projects/data228-final/topics/data228-in"

table_spec = bigquery.TableReference(
    projectId='data228-final',
    datasetId='stock_data',
    tableId='stock_table_test')

table_schema = 'ticker:STRING,date:STRING,open:FLOAT,high:FLOAT,low:FLOAT,close:FLOAT,adj_close:FLOAT,volume:INTEGER'

with beam.Pipeline(options=options) as pipeline:            

    data = pipeline | "read" >> beam.io.ReadFromPubSub(topic=topic_stream)
    windowed_data = (data | "window" >> beam.WindowInto(beam.window.FixedWindows(500))
                          | "reading" >> beam.Map(lambda x: json.loads(str(x,"utf-8")))
    )


    bq_write = (windowed_data | "streaming write" >> beam.io.WriteToBigQuery(table_spec, schema = table_schema,
                                                                             write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                                                                             create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED))

    DataflowRunner().run_pipeline(pipeline, options=options)

