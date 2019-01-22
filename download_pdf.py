import datetime
import logging
import multiprocessing

import os
import boto3
import botocore
import pandas as pd
import requests

FORMAT = '%(asctime)-15s %(clientip)s %(user)-8s %(message)s'
logging.basicConfig(format=FORMAT)
PATH = "data/"


def downlaod(names):
    filename, link = names
    response = requests.get(link, stream=True)
    with open(PATH+filename+".pdf", "wb") as pdf:
        for chunk in response.iter_content(chunk_size=1024):
            if chunk:
                pdf.write(chunk)


def pdfs_to_downlaod():
    link_filename = []
    try:
        csv_filename = str(datetime.date.today()) + '.json'
        data = pd.read_csv(csv_filename)
        pdf_links = data["pdf_link"]
        pdf_name = data["arxiv_id"]
        for filename, link in zip(pdf_name, pdf_links[1:2]):
            link_filename.append((filename, link))
        return link_filename
    except Exception as e:
        print(e)
        print("Probably arxiv RSS is down!!!")


def pdf_downlaod_main(pool):
    
    names = pdfs_to_downlaod()
    names = names
    try:
        pool.map(downlaod, names)
        pool.close()
        pool.join()
    except Exception as e:
        print(e)
        
        pass


def s3_to_local_machine(s3):
    BUCKET_NAME = 'researchkernel_datalake'  # replace with your bucket name
    KEY = 'rss/json/' + str(datetime.date.today()) + '.json'  # replace with your object key
    try:
        s3.Bucket(BUCKET_NAME).download_file(KEY, str(datetime.date.today()) + '.json')
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            print("The object does not exist.")
        else:
            raise

if __name__ == "__main__":

    s3 = boto3.resource('s3')
    pool = multiprocessing.Pool()

    s3_to_local_machine(s3)
    try:
        os.mkdir("data")
    except Exception as e:
        print(e)

    pdf_downlaod_main(pool)
    

    




