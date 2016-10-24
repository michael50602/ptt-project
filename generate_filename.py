import glob
import re

#! this generate file assume user has mounted the bucket to the file system, using gcsfuse
usr = "michael50602"
bucket = "ptt-source-posu-cto-1"
file_pattern = "/pttsource/G*"
file_name_path = "/pttsource/file_name.txt"
file_list = glob.glob("/home/"+ usr + '/' + bucket + file_pattern)
f_output = open("/home/"+ usr + '/' + bucket + file_name_path, 'w')
num = 0
for f in file_list:
    f_name = re.search(r"/home/"+usr+r'/'+bucket + r"/(?P<file_name>.*)", f).group("file_name")
    f_output.write(f_name + '\n')
    num += 1
print "number of files is %d"%(num)
f_output.close()
