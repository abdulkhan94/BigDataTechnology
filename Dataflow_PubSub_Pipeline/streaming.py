import apache_beam as beam
from apache_beam.options import pipeline_options
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.runners import DataflowRunner

import google.auth
from datetime import datetime, timedelta
import json


# Setting up the Apache Beam pipeline options.
options = pipeline_options.PipelineOptions(flags=['--streaming'])

options.view_as(pipeline_options.StandardOptions).streaming = True
_, options.view_as(GoogleCloudOptions).project = google.auth.default()

options.view_as(GoogleCloudOptions).region = 'us-west1'
options.view_as(GoogleCloudOptions).staging_location = 'gs://abdul-dataflow/staging'
options.view_as(GoogleCloudOptions).temp_location = 'gs://abdul-dataflow/temp'

#options.view_as(pipeline_options.SetupOptions).sdk_location = (
#            f'/root/apache-beam-custom/packages/beam/sdks/python/dist/apache-beam-{beam.version.__version__}0.tar.gz' )


topic = "projects/data228/topics/data228-hw8-in"


with beam.Pipeline(options=options) as pipeline:

    data = pipeline | "read" >> beam.io.ReadFromPubSub(topic=topic)
    windowed_data = (data | "window" >> beam.WindowInto(beam.window.FixedWindows(500))
                          | "reading" >> beam.Map(lambda x: json.loads(str(x,"utf-8")))
                          | "dict" >> beam.Map(lambda y: (y['id'], y['steps']))  
            
            )

    windowed_sum = (windowed_data 
     | "sum1" >> beam.CombinePerKey(sum)
	)

    windowed_avg = (windowed_data
     | "avg1" >> beam.CombinePerKey(beam.combiners.MeanCombineFn())
	)


    class PrintWindowResults(beam.DoFn):
        def process(self, element, window=beam.DoFn.WindowParam):
            new_element = element
            yield new_element



    ( windowed_sum
            | "sum4" >> beam.ParDo(PrintWindowResults())
            | "sum5" >> beam.Map(lambda st: '{{"id": {}, "total_steps": {}}}'.format(st[0],st[1]))
            | "sum6" >> beam.Map(lambda z: bytes(z, "utf-8"))
            | "sum7" >> beam.io.WriteToPubSub(topic="projects/data228/topics/data228-hw8-out")
    )


    ( windowed_avg  
        | "avg4" >> beam.ParDo(PrintWindowResults())
        | "avg5" >> beam.Map(lambda av: '{{"id": {}, "average_steps": {}}}'.format(av[0],av[1]))
        | "avg6" >> beam.Map(lambda po: bytes(po, "utf-8"))
        | "avg7" >> beam.io.WriteToPubSub(topic="projects/data228/topics/data228-hw8-out")
    )

    DataflowRunner().run_pipeline(pipeline, options=options)

