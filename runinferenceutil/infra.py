# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.

import logging

import apache_beam as beam
import torch
from apache_beam.ml.inference.base import RunInference, PredictionResult
from apache_beam.ml.inference.pytorch_inference import PytorchModelHandlerTensor
from apache_beam.ml.inference.pytorch_inference import make_tensor_model_fn
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.pvalue import AsDict, AsSingleton, EmptySideInput, AsIter, AsList
from apache_beam.runners.runner import PipelineResult
from apache_beam.transforms import window
from apache_beam.transforms.trigger import AfterProcessingTime, AccumulationMode
from apache_beam.utils.timestamp import Duration
from transformers import AutoConfig
from transformers import AutoModelForSeq2SeqLM
from transformers import AutoTokenizer
from datetime import datetime
import argparse

MAX_RESPONSE_TOKENS = 256

model_name = "google/flan-t5-base"
tokenizer = AutoTokenizer.from_pretrained(model_name)



# coding=utf-8
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# pytype: skip-file
# pylint:disable=line-too-long

class FormatForBigquery(beam.DoFn):
    def process(self, element, side,  window=beam.DoFn.WindowParam):

        ts_format = '%Y-%m-%d %H:%M:%S.%f UTC'
        window_start = window.start.to_utc_datetime().strftime(ts_format)
        window_end = window.end.to_utc_datetime().strftime(ts_format)
        logging.info(f"FormatForBigquery window_start: {window_start}, window_end: {window_end}")

        for i in side:
            now = datetime.now()  # current date and time
            date_time = now.strftime("%Y-%m-%d %H:%M:%S")

            item = element['Yes']
            percent, total = element['Yes'][0], element['Yes'][1]

            prompt = i.get('prompt')

            return [{
                'time': date_time,
                'prompt' : prompt,
                'totalMessages': total,
                'detectionPercent': percent,
            }]

class FormatForBigqueryMessages(beam.DoFn):
    def process(self, element):
        now = datetime.now()  # current date and time
        date_time = now.strftime("%Y-%m-%d %H:%M:%S")

        prediction, text = element[0], element[1]

        return [{
            'time': date_time,
            'prompt': text,
            'modelName' : model_name,
            'isDetected': prediction,
        }]

class PercentagesFn(beam.CombineFn):
    def create_accumulator(self):
      accumulator = {}
      return {}

    def add_input(self, accumulator, input):
      if input[0] not in accumulator:
        accumulator[input[0]] = 0  # {'🥕': 0}
      accumulator[input[0]] += 1  # {'🥕': 1}
      return accumulator

    def merge_accumulators(self, accumulators):
      merged = {}
      for accum in accumulators:
        for item, count in accum.items():
          if item not in merged:
            merged[item] = 0
          merged[item] += count
      return merged

    def extract_output(self, accumulator):

      total = sum(accumulator.values())  # 10
      percentages = {item: [round(count * 100 / total, 2), total] for item, count in accumulator.items()}
      return percentages


class ParDoMerge(beam.DoFn):
    def process(self, element,  side, window=beam.DoFn.WindowParam):
        ts_format = '%Y-%m-%d %H:%M:%S.%f UTC'
        window_start = window.start.to_utc_datetime().strftime(ts_format)
        window_end = window.end.to_utc_datetime().strftime(ts_format)
        logging.info(f"ParDoMerge window_start: {window_start}, window_end: {window_end}")

        for i in side:
            # print(f"Main {e.decode('utf-8')} Side {i}")
            # print(f"the side input extracted text {i.get('prompt')}")
            yield i.get('prompt') + '"' + element.decode('utf-8') + '"'

def to_bqrequest(e, sql):
    from apache_beam.io import ReadFromBigQueryRequest
    yield ReadFromBigQueryRequest(query=sql)


def loadstoremodel():
    state_dict_path = "saved_model"
    model_name = "google/flan-t5-base"
    # Load pre-trained model from hugging face registry or local disk
    model = AutoModelForSeq2SeqLM.from_pretrained(
            model_name, torch_dtype=torch.bfloat16
    )
    #Save Model in local disk
    torch.save(model.state_dict(), state_dict_path)



def to_tensors(input_text: str) -> torch.Tensor:
    """Encodes input text into token tensors.
    Args:
        input_text: Input text for the LLM model.
        tokenizer: Tokenizer for the LLM model.
    Returns: Tokenized input tokens.
    """

    return tokenizer.encode_plus(text=input_text,
                                 max_length=100,
                                 add_special_tokens=True, padding='max_length',
                                 return_attention_mask=True,
                                 return_token_type_ids=False,
                                 return_tensors="pt").input_ids[0]

def from_tensors(result: PredictionResult) -> tuple[str, str]:
    """Decodes output token tensors into text.
    Args:
        result: Prediction results from the RunInference transform.
        tokenizer: Tokenizer for the LLM model.
    Returns: The model's response as text.
    """
#    PredictionResult
    input_tokens = result.example
    decoded_inputs = tokenizer.decode(
         input_tokens, skip_special_tokens=True)

    decoded_outputs = tokenizer.decode(result.inference, skip_special_tokens=True)
    prompt_and_result = f"Input: {decoded_inputs} \t Output: {decoded_outputs}"
    print(prompt_and_result)
    logging.info('`runinference` : %s', prompt_and_result)
    #return decoded_outputs
    return (decoded_outputs, decoded_inputs)

def is_Alert(element, testAlert:list):
    #if there are Alerts in the current block then write all the messages to the tabel for further investigation
    if(len(testAlert) > 0) :
       return element
    else:
        return #return nothing

def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press ⌘F8 to toggle the breakpoint.


# See PyCharm help at https://www.jetbrains.com/help/pycharm/
