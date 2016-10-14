# coding: utf8

import tarfile
import re

t = tarfile.open('./toy-src/Gossiping.tar', 'r')
fail = 0
num_text = 0
for m in t.getmembers():
    if m.isfile():
        f = t.extractfile(m)
        content = f.read()
        clear_str = re.sub('\\x1B.*?[\\x41-\\x5A\\x61-\\x7A]', '', content)
        if re.match(".*text\\.txt", m.name):
            continue
            num_text += 1
            clear_str = clear_str.decode('big5', errors = 'ignore').encode('UTF-8', errors='ignore')
            pattern = re.compile("作者: (?P<author>.*) \\((?P<nick_name>.*)\\) 看板: (?P<board>.*)\\n標題:(?P<topic>.*)\\n時間: (?P<time>[^\\n]*)\\n(?P<content>(?:.|\\n)*)(?:From|來自): (?P<ip>.*).*")
            result = pattern.search(clear_str)
            if result is None:
                fail += 1
                if fail > 2:
                    break
            else :
                print result.groupdict().values()[2]
        if re.match(".*push\\.txt", m.name):
            clear_str = clear_str.decode('big5', errors = 'ignore').encode('UTF-8', errors='ignore')
            line = clear_str.split('\n')
            after_sub = re.sub('\\t{2}', '\\t', line[0])
            for i, w in enumerate(after_sub.split('\t')):
                print '%d th is %r'%(i, w)
                print w
                print re.match("^[推噓→]", w)
print fail
print num_text 
