import argparse
import time
import logging
import json

import apache_beam as beam
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.transforms import window


def decoder(message_bytes):
    message_list=message_bytes.decode("utf-8")
    message_str=message_list.strip("[]")
    message_json=json.loads(message_str)
    return message_json

class AddWindowEndFn(beam.DoFn):
    def process(self, element, window=beam.DoFn.WindowParam):
        yield (str(window.end.to_utc_datetime()), element)

class GetTimestampFn(beam.DoFn):
    def process(self, element, window=beam.DoFn.WindowParam):
        window_start = window.start.to_utc_datetime().strftime("%Y-%m-%dT%H:%M:%S")
        output = {'page_views': element, 'timestamp': window_start}
        yield output

class StatCalculatorFn(beam.DoFn):
    def process(self, group_with_key):
        group=group_with_key[1]
        key=group_with_key[0]
        count=len(group)
        number_of_male=0
        total_age=0
        total_register_age=0
        for i in group:
            if i["gender"]=="male":
                number_of_male=number_of_male+1
            total_age=total_age+i['dob']['age']
            total_register_age=total_register_age+i['registered']['age']
        

        number_of_female=count-number_of_male
        avg_age=total_age/count
        avg_reg_age=total_register_age/count


        yield {"timestamp":key,
               "Number_of_customer":count,
               "number_of_male":number_of_male,
               "number_of_female":number_of_female,
               "avg_customer_age":avg_age,
               "avg_customer_registred_age":avg_reg_age}


def run():

    parser = argparse.ArgumentParser(description='Load from Json from Pub/Sub into BigQuery')
    parser.add_argument('--project',required=True, help='Specify Google Cloud project')
    parser.add_argument('--region', required=True, help='Specify Google Cloud region')
    parser.add_argument('--staging_location', required=True, help='Specify Cloud Storage bucket for staging')
    parser.add_argument('--temp_location', required=True, help='Specify Cloud Storage bucket for temp')
    parser.add_argument('--runner', required=True, help='Specify Apache Beam Runner')
    parser.add_argument('--input_topic', required=True, help='Input Pub/Sub Topic')
    parser.add_argument('--agg_table_name', required=True, help='BigQuery table name for aggregate results')
    parser.add_argument('--window_duration', required=True, help='Window duration')

    opts = parser.parse_args()

    # Setting up the Beam pipeline options
    options = PipelineOptions(save_main_session=True, streaming=True)
    options.view_as(GoogleCloudOptions).project = opts.project
    options.view_as(GoogleCloudOptions).region = opts.region
    options.view_as(GoogleCloudOptions).staging_location = opts.staging_location
    options.view_as(GoogleCloudOptions).temp_location = opts.temp_location
    options.view_as(GoogleCloudOptions).job_name = '{0}{1}'.format('streaming-minute-customer-pipeline-',time.time_ns())
                                            
    options.view_as(StandardOptions).runner = opts.runner

    input_topic = opts.input_topic
    agg_table_name = opts.agg_table_name

    # Table schema for BigQuery
    agg_table_schema = {
        "fields": [
            {
                "name": "timestamp",
                "type": "TIMESTAMP"
            },
            {
                "name": "Number_of_customer",
                "type": "INTEGER"
            },
            {
                "name": "number_of_male",
                "type": "INTEGER"
            },
            {
                "name": "number_of_female",
                "type": "INTEGER"
            },
            {
                "name": "avg_customer_age",
                "type": "FLOAT"
            },
            {
                "name": "avg_customer_registred_age",
                "type": "FLOAT"
            },   
        ]
    }


    # Create the pipeline
    p = beam.Pipeline(options=options)

    (p  | 'ReadFromPubSub' >> beam.io.ReadFromPubSub(input_topic)
        | "decode">>beam.Map(decoder)
        | 'Apply Fixed 1 min Window ' >> beam.WindowInto(window.FixedWindows(60)) #window into one min
        | 'Add Window End Timestamp' >> beam.ParDo(AddWindowEndFn()) # add window end timestamp as key
        | 'Group by Window' >> beam.GroupByKey() # Group by using window end key
        | 'Group By Window End' >> beam.ParDo(StatCalculatorFn()) #Calculating insights from each 1 min window
        | 'WriteToBQ' >> beam.io.WriteToBigQuery(
                        agg_table_name,
                        schema=agg_table_schema,
                        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
                        
    )


    logging.getLogger().setLevel(logging.INFO)
    logging.info("Building pipeline ...")

    p.run().wait_until_finish()

if __name__ == '__main__':
  run()
