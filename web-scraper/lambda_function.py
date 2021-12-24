# coding: utf-8
import os
import re
import io
import csv
import json
import uuid
import boto3
import requests
from datetime import datetime
from bs4 import BeautifulSoup

aws_access_key_id=os.getenv('aws_access_key_id')
aws_secret_access_key=os.getenv('aws_secret_access_key')

def dollar2float(s):
    """Convert a dollar amount to a float
    
    Expecting format '$10.00'
    """

    digits = re.findall('[0-9]*\.[0-9]*', s)

    # capture group
    if digits:
        return float(digits[0])
    
    return 0.0

def get_headers(soup):
    """Get the headers from the table"""

    thead = soup.find('thead')

    table_headers = thead.find_all('th')

    return [th.text for th in table_headers]

def last_100_row_func(row):
    """Processes row from last 100 page"""

    # get relevant columns
    row = row[:7]
    # convert date to datetime object
    row[3] = datetime.strptime(row[3], '%m/%d/%Y')
    # convert shares 
    row[4] = float(row[4])
    # convert dollars to floats
    row[5] = dollar2float(row[5]) # offer price
    row[6] = dollar2float(row[6]) # first day close

    return row

def upcoming_ipo_row_func(row):
    """Processes row from calendar(upcoming) ipo page"""

    # get relevant columns
    row = row[:8]

    # convert shares to float
    row[3] = float(row[3])
    # convert prices to floats
    row[4] = dollar2float(row[4])
    row[5] = dollar2float(row[5])

    # convert date
    date_match = re.findall('[0-9]{1,2}\/[0-9]{1,2}\/[0-9]{2,4}', row[7])
    row[7] = datetime.strptime(date_match[0], '%m/%d/%Y')

    return row

def get_rows(soup, row_func=lambda row: row):
    """Extract data from the table
    
    Args:
        soup(bs4.Soup): Beautiful Soup
        row_func(function): Function to process individual rows of data
        
    Returns:
        rows(List[List]): Table rows represented as lists """

    tbody = soup.find('tbody')
    trs = tbody.find_all('tr')

    rows = []
    # clean the data and append to a list
    # each row is a line in the csv
    for tr in trs:
        # clean table data by getting only texting and stripping whitespace
        data = [td.text.strip() for td in tr.find_all('td')]
        # process row
        row = row_func(data)
        # append the row to the result set
        rows.append(row)

    return rows

def to_csv(headers, rows):
    """Convert headers and rows to single csv file
    
    Args:
        headers(list): column headers
        rows(list[list]): list of lists
    """

    buffer = io.StringIO()

    writer = csv.writer(buffer)

    # write the headers to the buffer string
    writer.writerow(headers)

    # write the rows
    writer.writerows(rows)

    # convert to bytes for upload
    bytes_buffer = io.BytesIO(buffer.getvalue().encode('utf-8'))

    return bytes_buffer

def upload_to_s3(name, file):
    """Upload file to s3"""

    # initialize the session with secret keys
    session = boto3.Session(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )
    # get the s3 resource
    s3 = session.resource('s3')
    # specify the bucket to upload to
    bucket = s3.Bucket('ipos-4c98')
    # upload the file with given name
    bucket.upload_fileobj(file, name)

def upcoming_ipos():
    """Process upcoming IPOs"""

    # upcoming
    url = "https://www.iposcoop.com/ipo-calendar/"
    # get page and convert to soup
    page = requests.get(url)
    soup = BeautifulSoup(page.text, 'html.parser')

    # get the headers
    headers = get_headers(soup)

    # trim down the headers
    if headers:
        headers = headers[:8]

    # get the rows
    rows = get_rows(soup, upcoming_ipo_row_func)

    # create name
    name = 'upcoming_ipos' + str(uuid.uuid4()) + '.csv'

    bcsv = to_csv(headers, rows)

    upload_to_s3(name, bcsv)

def previous_ipos():
    """Process last 100 IPOs"""

    # previous ipos
    url = 'https://www.iposcoop.com/last-100-ipos/'

    # get page and convert to soup
    page = requests.get(url)
    soup = BeautifulSoup(page.text, 'html.parser')

    # get the headers
    headers = get_headers(soup)

    # trim down the headers
    if headers:
        headers = headers[:7]

    # get the rows
    rows = get_rows(soup, last_100_row_func)

    # create name
    name = 'last_100_ipos' + str(uuid.uuid4()) + '.csv'

    bcsv = to_csv(headers, rows)

    upload_to_s3(name, bcsv)

def lambda_handler(event, context):
    # run ipo funcs
    upcoming_ipos()
    previous_ipos()

    return {
        'statusCode': 200,
        'body': json.dumps('Upload Successful')
    }

if __name__ == '__main__':
    event = {}
    context = {}
    print(lambda_handler(event, context))