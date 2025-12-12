import re

# Exact string from the logs (excluding quotes)
log_line = "12.21M   0%  549.61kB/s   36:10:19"
regex = r'\s*([0-9.,]+[a-zA-Z]?)\s+(\d+)%\s+([0-9.]+[kMGTP]?B/s)\s+([0-9:]+)'
pattern = re.compile(regex)

match = pattern.search(log_line)
if match:
    print("MATCH!")
    print(f"Groups: {match.groups()}")
else:
    print("NO MATCH")
