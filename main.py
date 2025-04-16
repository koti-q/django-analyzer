import os, argparse, asyncio, re
from collections import defaultdict
from multiprocessing import Pool # i barely know how this module works, studied it just for this app


def process_line(line):  
    if "django.request" not in line:
        return None
    log_match = re.search(r'\b(DEBUG|INFO|WARNING|ERROR|CRITICAL)\b', line) # looking for log level
    handler_match = re.search(r'(\/[a-zA-Z0-9\/_\-\.]+)', line) # looking for handler
    if not (log_match and handler_match):
        return None
    return (handler_match.group(1), log_match.group(1))

def process_file(path):
    local_report = defaultdict(lambda: defaultdict(int))

    with open(path, 'r') as f:
        for line in f:  # reading file by lines instead of loading all file to the memory
            result = process_line(line)
            if result:
                handler, log_level = result
                local_report[handler][log_level] += 1

    return dict(local_report)

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
    
    table_str = []
    table_str.append(row_format.format(*headers))
    table_str.append("-" * (sum(col_widths) + 3 * (len(headers) - 1)))
    
    for row in rows:
        table_str.append(row_format.format(*row))
    
    table_output = "\n".join(table_str)
    
    print(table_output)
    return table_output

if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog='analyze-logs', description='A CLI to analyze django logs.')
    parser.add_argument('log', nargs='+', type=str, 
                       help='A path to the log file. May be specified several paths')
    parser.add_argument('-r', '--report', type=str, help='An output file to save report.')
    parser.add_argument('-j', '--jobs', type=int, default=os.cpu_count(), 
                       help='Number of parallel jobs to use (default: number of CPU cores)')

    args = parser.parse_args()

    with Pool(args.jobs) as pool:
        reports = pool.map(process_file, args.log)

    merged_report = merge_reports(reports)
    table_output = table(merged_report)  
    
    if args.report:
        with open(args.report, 'w') as f:
            f.write(table_output) 
        print(f"\nReport saved to: {args.report}")