import os
import argparse
import asyncio
import re
from collections import defaultdict
from multiprocessing import Pool
from typing import Dict, Tuple, Optional, List, DefaultDict, Any


def process_line(line: str) -> Optional[Tuple[str, str]]:  
    if "django.request" not in line:
        return None
    log_match = re.search(r'\b(DEBUG|INFO|WARNING|ERROR|CRITICAL)\b', line)  # looking for log level
    handler_match = re.search(r'(\/[a-zA-Z0-9\/_\-\.]+)', line)  # looking for handler
    if not (log_match and handler_match):
        return None
    return (handler_match.group(1), log_match.group(1))

def process_file(path: str) -> Dict[str, Dict[str, int]]:
    local_report: DefaultDict[str, DefaultDict[str, int]] = defaultdict(lambda: defaultdict(int))

    with open(path, 'r') as f:
        for line in f:  # reading file by lines instead of loading all file to the memory
            result = process_line(line)
            if result:
                handler, log_level = result
                local_report[handler][log_level] += 1

    return {k: dict(v) for k, v in local_report.items()}

def merge_reports(report: List[Dict[str, Dict[str, int]]]) -> Dict[str, Dict[str, int]]:
    merged: DefaultDict[str, DefaultDict[str, int]] = defaultdict(lambda: defaultdict(int)) 
    
    for r in report:
        for handler, log_counts in r.items():
            for log_level, count in log_counts.items():
                merged[handler][log_level] += count

    return {
        handler: dict(log_level)
        for handler, log_level in merged.items()
    }

def table(report: Dict[str, Dict[str, int]]) -> str:
    headers: List[str] = ["HANDLER", "DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
    levels: List[str] = headers[1:] 
    sorted_handlers: List[str] = sorted(report.keys())  # sorting handlers 
    
    rows: List[List[str]] = []
    for handler in sorted_handlers:
        counts = report[handler]
        row = [handler]
        for level in levels:
            count = counts.get(level, 0)
            row.append(str(count))
        rows.append(row)

    footer: List[str] = ["TOTAL"]
    for level in levels:
        total = sum(report[handler].get(level, 0) for handler in sorted_handlers)
        footer.append(str(total))
    col_widths: List[int] = [
        max(len(str(row[i])) for row in [headers] + rows)
        for i in range(len(headers))
    ]
    col_widths[0] = max(col_widths[0], 20) 

    row_format: str = " | ".join(
        ["{:<" + str(col_widths[0]) + "}"] + 
        ["{:>" + str(w) + "}" for w in col_widths[1:]]
    )
    
    table_str: List[str] = []
    table_str.append(row_format.format(*headers))
    table_str.append("-" * (sum(col_widths) + 3 * (len(headers) - 1)))
    
    for row in rows:
        table_str.append(row_format.format(*row))

    table_str.append("-" * (sum(col_widths) + 3*(len(headers)-1)))
    table_str.append(row_format.format(*footer))
    
    table_output: str = "\n".join(table_str)
    
    print(table_output)
    return table_output

if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog='analyze-logs', description='A CLI to analyze django logs.')
    parser.add_argument('log', nargs='+', type=str, 
                       help='A path to the log file. May be specified several paths')
    parser.add_argument('-r', '--report', type=str, help='An output file to save report.')
    parser.add_argument('-j', '--jobs', type=int, default=os.cpu_count(), 
                       help='Number of parallel jobs to use (default: number of CPU cores)')

    args: argparse.Namespace = parser.parse_args()

    with Pool(args.jobs) as pool:
        reports: List[Dict[str, Dict[str, int]]] = pool.map(process_file, args.log)

    merged_report: Dict[str, Dict[str, int]] = merge_reports(reports)
    table_output: str = table(merged_report)  
    
    if args.report:
        with open(args.report, 'w') as f:
            f.write(table_output) 
        print(f"\nReport saved to: {args.report}")