import os
import re
import csv
import boto3
import hashlib

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from models import Base
from models import IPOs, UpcomingIPOs

username = os.getenv('mariadb_username')
password = os.getenv('mariadb_password')
host = os.getenv('mariadb_host')
database = os.getenv('mariadb_database')
aws_access_key_id=os.getenv('aws_access_key_id')
aws_secret_access_key=os.getenv('aws_secret_access_key')
port = 3306

engine = create_engine(f'mysql+pymysql://{username}:{password}@{host}:{port}/{database}')
Base.metadata.create_all(engine)

def get_s3_bucket():
    """Get the s3 bucket"""

    # initialize the session with secret keys
    session = boto3.Session(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )
    # get the s3 resource
    s3 = session.resource('s3')
    # specify the bucket to upload to
    bucket = s3.Bucket('ipos-4c98')

    return bucket

def clean_header(header):
    """Clean a header from CSV

    Args:
        header(str): string with special characters, spaces etc.
    Returns:
        clean_header(str): a cleaned string
    """

    header = header.lower()

    # remove special chars, periods and spaces
    header = re.sub('[$\.\s()]', '_', header)
    # replace any multi underlines with single _
    header = re.sub('_+', '_', header)
    # remove trailing _
    header = header.strip('_')

    return header

def load_file(file):
    """An s3 file object"""

    # download the file contents
    downloaded_file = file.get()
    # get the body object as a stream
    body_stream = downloaded_file['Body']

    # ready and decode the body_stream
    body = body_stream.read().decode()

    # split the body into lines
    lines = body.splitlines()

    # split off the first line as the headers
    headers = lines.pop(0)

    # TODO: clean the headers
    cleaned_headers = [
        clean_header(header) for header in headers.split(',')
    ]

    # read the csv into dict structure
    reader = csv.DictReader(lines, fieldnames=cleaned_headers)

    session = Session(engine)
    # iterate through rows in reader
    for row in reader:
        # get the line number from the reader
        # reader is 1 indexed on line 1
        # prior to getting line 1, it's 0 indexed
        str_line = lines[reader.line_num - 1]

        hashed_value = hashlib.sha256(
            str_line.encode()
        ).hexdigest()
        # query table based on files name and symbol
        # compare hashed_value to fingerprint
        # if values match, move on
        # if no value found, insert record
        # if values don't match, delete the record and insert the new one

        if file.key.startswith('upcoming'):
            # print(row)
            res = session.query(
                    UpcomingIPOs.symbol,
                    UpcomingIPOs.fingerprint
            ).filter(
                UpcomingIPOs.symbol == row['symbol_proposed']
            ).first()

            if res:
                if res[1] != hashed_value:
                    # delete existing record
                    session.query(
                        UpcomingIPOs
                    ).filter(
                        UpcomingIPOs.symbol == row['symbol_proposed']
                    ).delete()
                    # and add the new record
                    session.add(
                        UpcomingIPOs(
                            company=row['company'],
                            symbol=row['symbol_proposed'],
                            lead_managers=row['lead_managers'],
                            shares_million=row['shares_millions'],
                            price_low=row['price_low'],
                            price_high=row['price_high'],
                            est_volume=row['est_volume'],
                            expected_to_trade=row['expected_to_trade'],
                            fingerprint=hashed_value
                        )
                    )
                else:
                    print(row['symbol_proposed'], 'skipped')

            else:
                session.add(
                    UpcomingIPOs(
                        company=row['company'],
                        symbol=row['symbol_proposed'],
                        lead_managers=row['lead_managers'],
                        shares_million=row['shares_millions'],
                        price_low=row['price_low'],
                        price_high=row['price_high'],
                        est_volume=row['est_volume'],
                        expected_to_trade=row['expected_to_trade'],
                        fingerprint=hashed_value
                    )
                )


        if file.key.startswith('last_100'):
            res = session.query(
                    IPOs.symbol,
                    IPOs.fingerprint
            ).filter(
                IPOs.symbol == row['symbol']
            ).first()

            if res:
                if res[1] != hashed_value:
                    # delete existing record
                    session.query(
                        IPOs
                    ).filter(
                        IPOs.symbol == row['symbol']
                    ).delete()
                    # and add the new record
                    session.add(
                        IPOs(
                            company=row['company'],
                            symbol=row['symbol'],
                            industry=row['industry'],
                            offer_date=row['offer_date'],
                            shares_million=row['shares_millions'],
                            offer_price=row['offer_price'],
                            first_day_close=row['1st_day_close'],
                            fingerprint=hashed_value
                        )
                    )
                else:
                    print(row['symbol'], 'skipped')

            else:
                session.add(
                    IPOs(
                        company=row['company'],
                        symbol=row['symbol'],
                        industry=row['industry'],
                        offer_date=row['offer_date'],
                        shares_million=row['shares_millions'],
                        offer_price=row['offer_price'],
                        first_day_close=row['1st_day_close'],
                        fingerprint=hashed_value
                    )
                )

        # commit everything
        session.commit()

def main(event=None, lambda_context=None):
    bucket = get_s3_bucket()

    s3_objects = bucket.objects.iterator()

    # iterate through pages and files in s3 bucket
    for page in s3_objects.pages():
        for s3_file in page:
            load_file(s3_file)
            s3_file.delete()

    return {
        'statusCode': 200,
        'body': json.dumps('Upload Successful')
    }

if __name__ == '__main__':
    main()