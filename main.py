import os, argparse, asyncio, re
from collections import defaultdict

def file_parse(path):
    lines = []
    with open(path) as l:
        for line in l.read().splitlines():
            if "django.request" in line:
                lines.append(line)
    return lines

def handlers_report(lines):
    report = {} # {handler:{type:count, type:count]}

    for line in lines:
        log_match = re.search(r'\b(DEBUG|INFO|WARNING|ERROR|CRITICAL)\b', line)
        log_level = log_match.group(1)

        handler_match = re.search(r'(\/[a-zA-Z0-9\/_\-\.]+)', line)
        handler = handler_match.group(1)
        if handler not in report:
            report[handler] = {log_level:1}
        elif log_level not in report[handler]:
            report[handler][log_level] = 1
        elif log_level in report[handler]:
            report[handler][log_level] += 1
    return dict(report)

def merge_reports(report):
    merged = defaultdict(lambda: defaultdict(int)) 
    
    for r in report:
        for handler, log_counts in r.items():
            for log_level, count in log_counts.items():
                merged[handler][log_level] += count

    return {
        handler: dict(log_level)
        for handler, log_level in merged.items()
    }

def table(report):
    headers = ["HANDLER", "DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
    levels = headers[1:] 
    
    rows = []
    for handler, counts in report.items():
        row = [handler]
        for level in levels:
            row.append(str(counts.get(level, 0)))
        rows.append(row)
    
    col_widths = [
        max(len(str(row[i])) for row in [headers] + rows)
        for i in range(len(headers))
        ]
    col_widths[0] = max(col_widths[0], 20) 

    row_format = " | ".join(
        ["{:<" + str(col_widths[0]) + "}"] + 
        ["{:>" + str(w) + "}" for w in col_widths[1:]]
    )
    print(row_format.format(*headers))
    print("-" * (sum(col_widths) + 3 * (len(headers) - 1)))

    for row in rows:
        print(row_format.format(*row))

if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog='analyze-logs',description='A CLI to analyze django logs.')
    parser.add_argument('log', nargs='+', type=str, 
                        help='A path to the log file. May be specified several paths')
    parser.add_argument('-r', '--report', type=argparse.FileType('w'), help='An output file to save report.')

    args = parser.parse_args()
    report = []

    for path in args.log:
        f = file_parse(path)
        report.append(handlers_report(f))

    print(table(merge_reports(report)))
    
    


