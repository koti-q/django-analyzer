import pytest
from collections import defaultdict
from io import StringIO
import tempfile
import os
from main import process_line, process_file, merge_reports, table

def test_process_line_with_django_request():
    line = "[ERROR] django.request: Internal Server Error: /api/users/"
    result = process_line(line)
    assert result == ("/api/users/", "ERROR")

def test_process_line_without_django_request():
    line = "[INFO] django.db.backends: Query executed"
    result = process_line(line)
    assert result is None

def test_process_line_with_invalid_format():
    line = "This is not a valid log line"
    result = process_line(line)
    assert result is None

def test_process_line_with_different_log_levels():
    test_cases = [
        ("[DEBUG] django.request: Debug message: /debug/", ("/debug/", "DEBUG")),
        ("[INFO] django.request: Info message: /info/", ("/info/", "INFO")),
        ("[WARNING] django.request: Warning message: /warning/", ("/warning/", "WARNING")),
        ("[CRITICAL] django.request: Critical message: /critical/", ("/critical/", "CRITICAL")),
    ]
    
    for line, expected in test_cases:
        assert process_line(line) == expected

# tests for process_file
def test_process_file_with_valid_logs():
    log_content = """[ERROR] django.request: Internal Server Error: /api/users/
[INFO] django.request: GET /api/users/ - 200
[ERROR] django.request: Not Found: /api/products/
[DEBUG] django.request: Debug info: /debug/
[INFO] django.db.backends: Query executed - should be ignored
"""
    
    with tempfile.NamedTemporaryFile(mode='w+', delete=False) as temp_file:
        temp_file.write(log_content)
        temp_file.flush()
        
        result = process_file(temp_file.name)
        
        expected = {
            "/api/users/": {"ERROR": 1, "INFO": 1},
            "/api/products/": {"ERROR": 1},
            "/debug/": {"DEBUG": 1},
        }
        
        assert result == expected
    
    os.unlink(temp_file.name)

def test_process_file_with_empty_file():
    with tempfile.NamedTemporaryFile(mode='w+', delete=False) as temp_file:
        temp_file.write("")
        temp_file.flush()
        
        result = process_file(temp_file.name)
        assert result == {}
    
    os.unlink(temp_file.name)

# tests for merge_reports
def test_merge_reports():
    reports = [
        {
            "/api/users/": {"ERROR": 1, "INFO": 2},
            "/api/products/": {"ERROR": 1},
        },
        {
            "/api/users/": {"ERROR": 2, "DEBUG": 1},
            "/api/orders/": {"INFO": 1},
        },
    ]
    
    expected = {
        "/api/users/": {"ERROR": 3, "INFO": 2, "DEBUG": 1},
        "/api/products/": {"ERROR": 1},
        "/api/orders/": {"INFO": 1},
    }
    
    assert merge_reports(reports) == expected

def test_merge_empty_reports():
    assert merge_reports([]) == {}
    assert merge_reports([{}, {}]) == {}

# tests for table generating 
def test_table_formatting():
    report = {
        "/api/users/": {"ERROR": 3, "INFO": 2, "DEBUG": 1},
        "/api/products/": {"ERROR": 1},
    }
    
    output = table(report)
    lines = output.split('\n')
    
    assert "/api/products/" in lines[2] or "/api/products/" in lines[3]
    assert "/api/users/" in lines[2] or "/api/users/" in lines[3]
    
    assert "TOTAL" in lines[-1]
    assert "4" in lines[-1]  

def test_integration(tmp_path):

    log1 = tmp_path / "log1.log"
    log2 = tmp_path / "log2.log"
    
    log1_content = """[ERROR] django.request: Error: /api/users/
[INFO] django.request: GET /api/users/
[DEBUG] django.request: Debug: /debug/
"""
    log2_content = """[ERROR] django.request: Error: /api/users/
[WARNING] django.request: Warning: /api/products/
"""
    
    log1.write_text(log1_content)
    log2.write_text(log2_content)
    
    report1 = process_file(str(log1))
    report2 = process_file(str(log2))
    
    merged = merge_reports([report1, report2])
    
    expected = {
        "/api/users/": {"ERROR": 2, "INFO": 1},
        "/debug/": {"DEBUG": 1},
        "/api/products/": {"WARNING": 1},
    }
    
    assert merged == expected
    
    table_output = table(merged)
    assert "/api/users/" in table_output
    assert "2" in table_output  # ERROR count for /api/users/
    assert "1" in table_output  # WARNING count for /api/products/