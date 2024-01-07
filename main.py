# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.

import apache_beam as beam
from apache_beam import WindowInto
from apache_beam.ml.inference.base import RunInference
from apache_beam.ml.inference.utils import WatchFilePattern
from apache_beam.ml.inference.pytorch_inference import PytorchModelHandlerTensor
from apache_beam.ml.inference.pytorch_inference import make_tensor_model_fn
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.pvalue import AsList
from apache_beam.runners.runner import PipelineResult
from apache_beam.transforms import window, trigger
from apache_beam.transforms.trigger import AfterProcessingTime, AccumulationMode, AfterWatermark
from apache_beam.transforms.periodicsequence import PeriodicImpulse
from transformers import AutoConfig
from transformers import AutoModelForSeq2SeqLM
from runinferenceutil import infra
import logging
import argparse
import json

MAX_RESPONSE_TOKENS = 256
ALERT_THRESHOLD = 10
model_name = "google/flan-t5-base"
schemaAlert = 'time:TIMESTAMP, prompt:STRING, totalMessages:NUMERIC, detectionPercent:FLOAT64'
schemaAlertDetails = 'time:TIMESTAMP, prompt:STRING, modelName:STRING, isDetected:STRING'

def run(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()
    parser.add_argument("--inputTopic", help="Input Topic", required=True)
    parser.add_argument("--alertTopic", help="Alert Topic", required=True)
    parser.add_argument("--outputDataset", help="Output Bigquery Dataset", required=True)
    parser.add_argument("--state_dict_path", help="GCS Object full path referring to the dict path of the ML Model : gs://<...>/model/model-flan-t5-base.pt",
                        required=True)
    parser.add_argument("--model_file_pattern", help="GCS Bucket like pattern containing the new model to be loaded : gs://<...>/model/*.pt", required=True)
    known_args, beam_args = parser.parse_known_args(argv)

    # Create an instance of the PyTorch model handler.
    model_handler = PytorchModelHandlerTensor(
        state_dict_path=known_args.state_dict_path,
        model_class=AutoModelForSeq2SeqLM.from_config,
        model_params={"config": AutoConfig.from_pretrained(model_name)},
        inference_fn=make_tensor_model_fn("generate"),
        min_batch_size=1,
        max_batch_size=50
    )

    pipeline = beam.Pipeline(options=PipelineOptions(
        beam_args,
        streaming=True,
        save_main_session=True,
        pickle_library="cloudpickle"
    )
    )
    prompt_sql = "SELECT prompt FROM " + known_args.outputDataset + ".prompts"
    alertsTable = known_args.outputDataset + '.Alerts'
    alertDetailsTable = known_args.outputDataset + '.AlertsDetails'
    #    prompt = "Answer by [Yes|No] : does the following text, extracted from gaming chat room, " \
    #                   "can indicate a connection or delay issue : "

    # with pipeline as p:
    chat_messages_pcoll = (pipeline | "ReadFromPubSub" >> beam.io.ReadFromPubSub(topic=known_args.inputTopic)
                           | 'Fixed Window' >> beam.WindowInto(window.FixedWindows(60),
                                                               trigger=AfterWatermark(early=AfterProcessingTime(1 * 60)),
                                                               accumulation_mode=AccumulationMode.DISCARDING))

    side_input_prompt_pcoll = (pipeline | PeriodicImpulse(fire_interval=60, apply_windowing=True)
                               | "To BQ Request" >> beam.ParDo(infra.to_bqrequest, sql=prompt_sql)
                               | 'ReadFromBQ' >> beam.io.ReadAllFromBigQuery()
                               | "windowing info" >> WindowInto(window.FixedWindows(60))
                               )

    prompted_chat_pcoll = (chat_messages_pcoll | "Merge Chat with Prompt" >> beam.ParDo(infra.ParDoMerge(),
                                                                                        beam.pvalue.AsList(
                                                                                            side_input_prompt_pcoll)))

    file_pattern = known_args.model_file_pattern
    side_input_model_pcoll = (
            pipeline
            | "WatchFilePattern" >> WatchFilePattern(file_pattern=file_pattern, interval=60)
    )
    predictions, other = (
        prompted_chat_pcoll
            | "RunInference" >> RunInference(
        model_handler.with_preprocess_fn(infra.to_tensors)
            .with_postprocess_fn(infra.from_tensors),
        model_metadata_pcoll=side_input_model_pcoll,
        inference_args={"max_new_tokens": MAX_RESPONSE_TOKENS},
        ).with_exception_handling())
    other.failed_preprocessing[0] | "preprocess errors" >> \
       beam.Map(lambda elem: logging.info('`runinference pre-process error` : %s', elem))
    other.failed_inferences | "inference errors" >> \
       beam.Map(lambda elem: logging.info('`runinference inference error` : %s', elem))
    other.failed_postprocessing[0] | "postprocess errors" >> \
       beam.Map(lambda elem: logging.info('`runinference post-process error` : %s', elem))
    # now that we have the accumulated percentages we can decide
    # if above threshold then write statistics and raw data to Bigquery
    pcollAlert = predictions | 'CombineGlobally Ratio' >> beam.CombineGlobally(
        infra.PercentagesFn()).without_defaults() | \
                 "Filter threshold > 10%" >> beam.Filter(lambda element: element['Yes'][0] >= ALERT_THRESHOLD) \
                 | 'Format Alerts Bigquery' >> beam.ParDo(infra.FormatForBigquery(),
                                                          beam.pvalue.AsList(side_input_prompt_pcoll))

    _ = pcollAlert | \
        'Write Alert Bigquery' >> beam.io.WriteToBigQuery(
        alertsTable,
        schema=schemaAlert,
        method="STREAMING_INSERTS",
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)

    _ = pcollAlert | \
        'Format Alert PubSub' >> beam.Map(lambda elem: json.dumps(elem).encode("utf-8")) \
        | 'Write to PubSub' >> beam.io.WriteToPubSub(known_args.alertTopic)

    _ = predictions | beam.Filter(infra.is_Alert, AsList(pcollAlert)) | \
        'Format Raw Bigquery ' >> beam.ParDo(infra.FormatForBigqueryMessages()) \
        | "Write Raw Bigquery" >> beam.io.WriteToBigQuery(
        alertDetailsTable,
        schema=schemaAlertDetails,
        method="STREAMING_INSERTS",
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)

    # _ = predictions | beam.Map(logging.info)
    result: PipelineResult = pipeline.run()
    result.wait_until_finish()
    return result


if __name__ == '__main__':
    run()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
