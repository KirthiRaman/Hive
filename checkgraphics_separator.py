import re

i=0
for line in sys.stdin:
    line = line.strip()
    #line_list = line.split("|")
    line_list = re.split("\xb3", line)
    print line_list[0]+"|"+line_list[1]+"|"+line_list[2]
