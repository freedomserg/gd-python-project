Web Crawler that parses https://blog.griddynamics.com/, obtains articles and author information from it. 
And generates a report with this information.

Web Crawler runs on python3

To run a crawler:
- checkout the project from github
- go to the project dir and create a new virtual environment:
    python3 -m venv <env_name>
- activate your virtual environment:
    source env/bin/activate
- install all dependencies: 
    pip3 install --upgrade -r requirements.txt
- run a crawler: 
    python3 report.py
    
To run unit tests:
python3 -m unittest -v tests/test_spiders.py tests/test_report_data.py


