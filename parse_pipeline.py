#!/usr/bin/python
# -*- coding: utf-8 -*-
import apache_beam as beam
from apache_beam.utils.options import PipelineOptions
from apache_beam.utils.options import SetupOptions
from apache_beam.internal.clients import bigquery
from apache_beam.io.iobase import Read
from apache_beam.io import fileio
from apache_beam.io import BigQuerySink 
from apache_beam.io import BigQueryDisposition
from apache_beam.io import range_trackers
import argparse
import logging

#! global variable setting they will move to Makefile in future
bucket = "ptt-source-posu-cto-1"

class ExtractFileContent(beam.DoFn):
    def create_service(self):
        from googleapiclient import discovery
        from oauth2client.client import GoogleCredentials
        credentials = GoogleCredentials.get_application_default()
        return discovery.build('storage', 'v1', credentials=credentials)
    def get_object(self, bucket, filename, out_file):
        from googleapiclient import http
        service = self.create_service()
        req = service.objects().get_media(bucket=bucket, object=filename)
        downloader = http.MediaIoBaseDownload(out_file, req)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
            logging.info("file {} Download {}%.".format(filename, int(status.progress() * 100)))
        return out_file

    def process(self, context):
        import tarfile
        import tempfile
        import re
        global bucket
        file_name = context.element
        f = tempfile.TemporaryFile()
        f = self.get_object(bucket, file_name, f)
        f.seek(0, 0)
        tar = tarfile.open(fileobj = f, mode='r')
        text_push_pair = {}
        for member in tar.getmembers():
            if member.isfile():
                file_id = member.name[:-9]
                if file_id not in text_push_pair.keys():
                    text_push_pair[file_id] = {'push':None, 'text':None}
                f = tar.extractfile(member)
                content = f.read()
                # classify whether the file is a push or text
                if re.search(".*text\\.txt", member.name):
                    text_push_pair[file_id]['text'] = content
                elif re.search(".*push\\.txt", member.name):
                    text_push_pair[file_id]['push'] = content
                else:
                    continue
        return text_push_pair.items() 

class Parse_Text_Members(beam.DoFn):
    def process(self, context):
        import re
        import datetime
        file_name, pt_dict = context.element
        # parse date from file_name
        parse_date = re.search("\\.(?P<milisecond>[0-9]*)\\.", file_name)
        if parse_date is None:
            return
        file_date = datetime.datetime.fromtimestamp( int(parse_date.groupdict()['milisecond'])/1000.0 )
        # parse text string
        text_str = pt_dict['text']
        text_str = re.sub('\\x1B.*?[\\x41-\\x5A\\x61-\\x7A]', '', text_str)
        text_str = text_str.decode('big5', errors = 'ignore')#.encode('UTF-8', errors = 'ignore')
        pattern = re.compile(u"作者: (?P<author>.*) \\((?P<nickname>.*)\\) 看板: (?P<board>.*)\\n標題:(?P<topic>.*)\\n時間: (?P<time>[^\\n]*)\\n(?P<content>(?:.|\\n)*)(?:From|來自): (?P<ip>.*).*", re.UNICODE)
        text_output = pattern.search(text_str)
        if text_output is not None:
            parse_result = text_output.groupdict()
            #parse_time = time.strptime(parse_result['text']['time'], '%a %b %d %H:%M:%S %Y')
            #parse_result['text']['time'] = datetime.datetime(parse_time.tm_year, parse_time.tm_mon, parse_time.tm_mday, parse_time.tm_hour, parse_time.tm_min, parse_time.tm_sec)
            parse_result['file_name'] = file_name
            return [parse_result]
        else:
            return


class Parse_Push_Members(beam.DoFn):
    def is_time_column(self, col):
        import re
        time_pat = re.compile(u"(?P<month>[0-9]{2})\/(?P<day>[0-9]{2}) ?(?P<hour>[0-9]{2})?:?(?P<minute>[0-9]{2})?", re.UNICODE)
        if time_pat.search(col[0]) is not None:
            return True
        else:
            return False

    def is_id_column(self, col):
        import re
        id_pat = re.compile(u"^[a-zA-Z0-9]+$", re.UNICODE)
        for c in col:
            if id_pat.search(c) is None:
                return False
        return True

    def is_type_column(self, col):
        import re
        type_pat = re.compile(u"^[推噓→]", re.UNICODE)
        for c in col:
            if type_pat.match(c) is None:
                return False
        return True
        
    def parse_push_format(self, lines):
        import datetime
        import re
        import numpy as np
        line_format = {'id': None, 'time': None, 'content': None, 'type': None}
        words = []
        normal_length = len(lines[0].split('\t'))
        for l in lines:
            process_line = l.split('\t')
            if len(l) > 0 and len(process_line) == normal_length:
                words.append(process_line)
        words = np.array(words)
        for i in range(4):
            if line_format['time'] is None and self.is_time_column(words[:, i]):
                line_format['time'] = i
                continue
            elif line_format['id'] is None and self.is_id_column(words[:, i]):
                line_format['id'] = i
                continue
            elif line_format['type'] is None and self.is_type_column(words[:, i]):
                line_format['type'] = i
                continue
            else:
                line_format['content'] = i
        return line_format

    def process(self, context):
        import re
        import datetime
        import numpy as np
        import time
        file_name, pt_dict = context.element
        parse_result = {'push': None, 'text': None}

        # parse date from file_name
        parse_date = re.search("\\.(?P<milisecond>[0-9]*)\\.", file_name)
        if parse_date is None:
            return
        file_date = datetime.datetime.fromtimestamp( int(parse_date.groupdict()['milisecond'])/1000.0 )
        # parse push string
        if pt_dict['push'] is not None: 
            push_str = pt_dict['push']
            push_str = re.sub('\\x1B.*?[\\x41-\\x5A\\x61-\\x7A]', '', push_str)
            push_str = re.sub('\\t{2}', '\\t', push_str) # remove a '\t' in push_str
            push_str = push_str.decode('big5', errors = 'ignore').encode('UTF-8', errors = 'ignore')
            lines = push_str.split('\n')
            push_list = [ {'time': None, 'id': None, 'type': None, 'content': None} for i in range(len(lines))]
            line_format = self.parse_push_format(lines) # parse which column is content, id, type & time 
            words = []
            normal_length = len(lines[0].split('\t'))
            for l in lines:
                process_line = l.split('\t')
                if len(l) > 0 and len(process_line) == normal_length:
                    words.append(process_line)
            words = np.array(words)
            time_index = line_format['time']
            id_index = line_format['id']
            type_index = line_format['type']
            content_index = line_format['content']
            time_pat = re.compile(u"(?P<month>[0-9]{2})\/(?P<day>[0-9]{2}) ?(?P<hour>[0-9]{2})?:?(?P<minute>[0-9]{2})?")
            id_pat = re.compile(u"^[a-zA-Z0-9]+$")
            type_pat = re.compile(u"^[推噓→]$")
            filter_push = []
            for i, t in enumerate(words[:, time_index]):
                result = time_pat.search(t)
                push_list[i]['time'] = 0
                try:
                    if result.groupdict()['hour'] is not None and result.groupdict()['minute'] is not None:
                        push_list[i]['time'] = datetime.datetime(file_date.year, int(result.groupdict()['month']), int(result.groupdict()['day']), int(result.groupdict()['hour']), int(result.groupdict()['minute']))
                    else:
                        push_list[i]['time'] = datetime.datetime(file_date.year, int(result.groupdict()['month']), int(result.groupdict()['day']))
                except:
                    filter_push.append(i)
                    continue
            for i, u in enumerate(words[:, id_index]):
                if i in filter_push:
                    continue
                push_list[i]['id'] = u
            for i, t in enumerate(words[:, type_index]):
                if i in filter_push:
                    continue
                t = str(t)
                if t == u'推':
                    push_list[i]['type'] = 1
                elif t == u'噓':
                    push_list[i]['type'] = -1
                else:
                    push_list[i]['type'] = 0
            for i, c in enumerate(words[:, content_index]):
                if i in filter_push:
                    continue
                push_list[i]['content'] = c
            parse_result['text'] = push_list
        return [{'mykey':[1, 2, 30]}]
        #return [("test-string")]


def run(argv = None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', dest = 'input', default = 'gs://ptt-source-posu-cto-1/pttsource/file_name.txt')
    parser.add_argument('--output', dest = 'output', default = 'gs://ptt-data/output')
    args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    p = beam.Pipeline(options=pipeline_options)
    text_schema = u"ip:STRING,file_name:STRING,content:STRING,board:STRING,time:STRING,topic:STRING,author:STRING,nickname:STRING"
    push_schema = u"mykey:RECORD"
    file_cont = p | "Read file name" >> beam.io.Read(beam.io.TextFileSource(args.input)) | "Extract File Content" >> beam.ParDo(ExtractFileContent())
    file_cont | "Parse Push Members" >> beam.ParDo(Parse_Push_Members()) | "Write Push to BigTable" >> beam.io.Write(beam.io.BigQuerySink(table="ptt_push", dataset="ptt_dataset", project="ptt-project", schema=push_schema))
    file_cont | "Parse Text Members" >> beam.ParDo(Parse_Text_Members()) #| "Write Text to BigTable" >> beam.io.Write(beam.io.BigQuerySink(table="ptt_text", dataset="ptt_dataset", project="ptt-project", schema=text_schema))
    #id_cont | "Write to BigQuery" >> beam.io.Write(beam.io.BigQuerySink(table="ptt_table", project="ptt-project", schema=schema, dataset="ptt_dataset", create_disposition='CREATE_IF_NEEDED'))
    p.run()

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
