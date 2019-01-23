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
    try:
        response = requests.get(link, stream=True)
        with open(PATH+filename+".pdf", "wb") as pdf:
            for chunk in response.iter_content(chunk_size=1024):
                if chunk:
                    pdf.write(chunk)
    except Exception as e:
        print(e)
        pass


def pdfs_to_downlaod():
    link_filename = []
    try:
        json_filename = str(datetime.date.today()) + '.json'
        data = pd.read_json(json_filename)
        data = data.T
        pdf_links = data["pdf_link"]
        pdf_name = data["arxiv_id"]
        for filename, link in zip(pdf_name, pdf_links):
            link_filename.append((filename, link))
        return link_filename
    except Exception as e:
        print(e)
        print("Probably arxiv RSS is down!!!")

def pdf_downlaod_main(pool):
    
    names = pdfs_to_downlaod()
    try:
        pool.map(downlaod, names)
        pool.close()
        pool.join()
    except Exception as e:
        print(e)
        pass

def s3_to_local_machine(s3):
    BUCKET_NAME = 'researchkernel-datalake' 
    KEY = 'rss/json/' + str(datetime.date.today()) + '.json'  
    try:
        s3.Bucket(BUCKET_NAME).download_file(KEY, str(datetime.date.today()) + '.json')
        print("Downloaded file!!!")
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            print("The object does not exist.")
        else:
            raise

def upload_data_s3():
    client = boto3.client('s3')
    BUCKET_NAME = 'researchkernel-datalake' 
    KEY = 'rss/json/' + str(datetime.date.today()) + '.json'  
    try:
        for root,dirs,files in os.walk("./data"):
            for file in files:
                client.upload_file(os.path.join(root,file),BUCKET_NAME,"rss/pdf/"+str(datetime.date.today())+"/"+file)
    except Exception as e:
        print(e)
        pass
if __name__ == "__main__":
    s3 = boto3.resource('s3')
    pool = multiprocessing.Pool()

    # downloading s3 to local machine 
    s3_to_local_machine(s3)

    # make a folder named data.
    try:
        os.mkdir("data")
    except Exception as e:
        print(e)
        pass
    # start downloading pdf into newly created folder 
    pdf_downlaod_main(pool)

    # Uploading PDF to S3
    upload_data_s3()