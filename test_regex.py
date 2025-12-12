import re

log_line = "          1.08G   1%    9.57MB/s    1:59:51  "
regex = r'\s*([0-9.,]+[a-zA-Z]?)\s+(\d+)%\s+([0-9.]+[kMGTP]?B/s)\s+([0-9:]+)'
pattern = re.compile(regex)

match = pattern.search(log_line)
if match:
    print("MATCH!")
    print(f"Groups: {match.groups()}")
else:
    print("NO MATCH")
