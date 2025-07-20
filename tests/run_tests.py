import os
import sys
import datetime
import subprocess

def run_tests():
    # Create timestamp
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Create reports directory if it doesn't exist
    reports_dir = os.path.join('reports', timestamp)
    os.makedirs(reports_dir, exist_ok=True)
    
    # Construct the pytest command
    python_exe = sys.executable
    xml_report = os.path.join(reports_dir, 'test_results.xml')
    html_report = os.path.join(reports_dir, 'test_results.html')
    
    # Run pytest with reports
    cmd = [
        python_exe, '-m', 'pytest',
        'test_data_transformation.py',
        '-v',
        f'--junitxml={xml_report}',
        f'--html={html_report}',
        '--self-contained-html'  # This makes the HTML report self-contained
    ]
    
    # Execute the tests
    subprocess.run(cmd)
    
    print(f"\nTest reports have been generated in the '{reports_dir}' directory:")
    print(f"- XML Report: {xml_report}")
    print(f"- HTML Report: {html_report}")

if __name__ == '__main__':
    run_tests()
