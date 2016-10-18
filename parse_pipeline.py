#!/usr/bin/python
# -*- coding: utf-8 -*-
import apache_beam as beam
from apache_beam.utils.options import PipelineOptions
from apache_beam.utils.options import SetupOptions
from apache_beam.io import filebasedsource
from apache_beam.transforms import PTransform
from apache_beam.io.iobase import Read
from apache_beam.io import fileio
from apache_beam.io import range_trackers
import argparse
import logging

class ExtractFileContent(beam.DoFn):
    def create_service(self):
        from googleapiclient import discovery
        from oauth2client.client import GoogleCredentials
        credentials = GoogleCredentials.get_application_default()
        return discovery.build('storage', 'v1', credentials=credentials)
    def get_object(self, bucket, filename, out_file):
        from googleapiclient import http
        service = self.create_service()
        req = service.objects().get_media(bucket='ptt-source-posu-cto-1', object='pttsource/G-baseball.20120606.tgz')
        downloader = http.MediaIoBaseDownload(out_file, req)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
            print("Download {}%.".format(int(status.progress() * 100)))
        return out_file

    def process(self, context):
        import tarfile
        import tempfile
        file_name = context.element
        f = tempfile.TemporaryFile()
        f = self.get_object('ptt-source-posu-cto-1', 'pttsource/G-baseball.20120606.tgz.txt', f)
        f.seek(0, 0)
        print f.tell()
        tar = tarfile.open(fileobj = f, mode='r')
        for m in tar.getmembers():
            logging.info("the file name is %s", m.name)
        return [(1)]


class Ptt_Compressed_Source(filebasedsource.FileBasedSource):
    def __init__(self, file_pattern, split, comp_type):
        super(Ptt_Compressed_Source, self).__init__(file_pattern, splittable=split, compression_type = comp_type)
    def estimate_size(self):
        logging.infor("estimate_size is %d", self._get_concat_source().estimate_size())
        return self._get_concat_source().estimate_size()
    def read_records(self, file_name, range_tracker):
        import tarfile
        import re
        import StringIO
        start_offset = range_tracker.start_position()
        f = self.open_file(file_name)
        start = range_tracker.start_position
        text_str = str(f.read(f._read_size))
        print len(text_str)
        print f._read_size
        #text_str = re.sub('\\x1B.*?[\\x41-\\x5A\\x61-\\x7A]', '', text_str)
        #text_str = text_str.decode('big5', errors = 'ignore').encode('UTF-8', errors = 'ignore')
        tar = tarfile.open(fileobj=StringIO.StringIO(text_str), bufsize=f._read_size)
        for mem in tar.getmembers():
            print mem.name
        return [(1)]
        # make key value format as { 'file_id': {'text': text_content, 'push': push_content} }
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
        return [(file_name, text_push_pair)] 

class Parse_Members(beam.DoFn):
    def is_time_column(self, col):
        import re
        time_pat = re.compile("(?P<month>[0-9]{2})\/(?P<day>[0-9]{2}) ?(?P<hour>[0-9]{2})?:?(?P<minute>[0-9]{2})?")
        if time_pat.search(col[0]) is not None:
            return True
        else:
            return False

    def is_id_column(self, col):
        import re
        id_pat = re.compile("^[a-zA-Z0-9]+$")
        for c in col:
            if id_pat.search(c) is None:
                return False
        return True

    def is_type_column(self, col):
        import re
        type_pat = re.compile("^[推噓→]")
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
        file_year = 0
        # parse text string
        text_str = pt_dict['text']
        text_str = re.sub('\\x1B.*?[\\x41-\\x5A\\x61-\\x7A]', '', text_str)
        text_str = text_str.decode('big5', errors = 'ignore').encode('UTF-8', errors = 'ignore')
        pattern = re.compile("作者: (?P<author>.*) \\((?P<nick_name>.*)\\) 看板: (?P<board>.*)\\n標題:(?P<topic>.*)\\n時間: (?P<time>[^\\n]*)\\n(?P<content>(?:.|\\n)*)(?:From|來自): (?P<ip>.*).*")
        text_output = pattern.search(text_str)
        if text_output is not None:
            parse_result['text'] = text_output.groupdict()
            parse_time = time.strptime(parse_result['text']['time'], '%a %b %d %H:%M:%S %Y')
            parse_result['text']['time'] = datetime.datetime(parse_time.tm_year, parse_time.tm_mon, parse_time.tm_mday, parse_time.tm_hour, parse_time.tm_min, parse_time.tm_sec)

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
            time_pat = re.compile("(?P<month>[0-9]{2})\/(?P<day>[0-9]{2}) ?(?P<hour>[0-9]{2})?:?(?P<minute>[0-9]{2})?")
            id_pat = re.compile("^[a-zA-Z0-9]+$")
            type_pat = re.compile("^[推噓→]$")
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
                if t == '推':
                    push_list[i]['type'] = 1
                elif t == '噓':
                    push_list[i]['type'] = -1
                else:
                    push_list[i]['type'] = 0
            for i, c in enumerate(words[:, content_index]):
                if i in filter_push:
                    continue
                push_list[i]['content'] = c
            parse_result['text'] = push_list
        return [(file_name, parse_result)]


def run(argv = None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', dest = 'input', default = 'gs://ptt-source-posu-cto-1/pttsource/Gossiping.20120606.tgz')
    parser.add_argument('--output', dest = 'output', default = 'gs://ptt-data/output')
    args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    p = beam.Pipeline(options=pipeline_options)
    #pcoll = p | "Read" >> beam.io.Read(Ptt_Compressed_Source(args.input, split = False, comp_type = fileio.CompressionTypes.GZIP))
    #pcoll | beam.ParDo('parse tar member', Parse_Members())
    lines = p | "Read file name" >> beam.io.Read(beam.io.TextFileSource(args.input))
    lines | "Extract file content" >> beam.ParDo(ExtractFileContent())
    p.run()

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
