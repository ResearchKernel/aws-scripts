import os
import sys
import time
import json
import shutil
import multiprocessing
from subprocess import call
import pdfx
import boto3

# Gobal DIR 
# "Change to according to your directory"

txt_path = "./data/text/"
ref_path = "./data/references/"
pdf_path = "./data/pdf/"
have = set(os.listdir(txt_path))

def pdf_dir():
    data = []
    for paths, dirs, file in os.walk(pdf_path):
        for f in file:
            data.append((paths, f))
    return data
 
def pdf_extract(dirs):
    '''Function takes filename and path to the file as a tuple and save the extracted text and references \
    from PDF file to txt_path dirs = ("pdf_data/", "filename.pdf")'''
    paths, filename = dirs
    file_ = filename.replace(".pdf", ".txt")
    file_json = filename.replace(".pdf", ".json")
    if file_ in have:
        print("file already extracted!!")
    else:
        print("read pdf file", filename)
        cmd_text_extractor = "pdfx %s -t -o %s" % (
            os.path.join(paths, filename), txt_path+file_)
        pdf = pdfx.PDFx(os.path.join(paths, filename))
        references_dict = pdf.get_references_as_dict()
        print("extrated reference of:", file_)
        os.system(cmd_text_extractor)
        print("extracted pdf_file:", file_)
        with open(ref_path+file_json, 'w') as fp:
            json.dump(references_dict, fp)
        print("save json to reference:", file_json)

def download_s3_pdf(bucket_name):
    '''
    download pdf from s3_pdf 
    '''
    s3 = boto3.client('s3')
    PDF_FILENAME = []
    PREFIX = 's3_pdf/pdf/2019-01-24/'
    result = s3.list_objects(Bucket=bucket_name,
                            Prefix=PREFIX,
                            Delimiter='/')
    try:
        for i in range(0,5000):
            PDF_FILENAME.append(result["Contents"][i]["Key"])
    except Exception as identifier:
        pass
    s3 = boto3.resource('s3')
    try:
        for i in PDF_FILENAME:
            s3.Bucket(bucket_name).download_file(i, pdf_path+i.replace('s3_pdf/pdf/2019-01-24/', ""))
    except Exception as e:
        print(e)
        pass    

def download_rss_pdf(bucket_name):
    '''
    Download pdf downloaded from rss
    '''
    s3 = boto3.client('s3')
    PDF_FILENAME = []
    PREFIX = 'rss/pdf/2019-01-24/'
    result = s3.list_objects(Bucket=bucket_name,
                            Prefix=PREFIX,
                            Delimiter='/')
    try:
        for i in range(0,5000):
            PDF_FILENAME.append(result["Contents"][i]["Key"])
    except Exception as identifier:
        pass
    s3 = boto3.resource('s3')
    try:
        for i in PDF_FILENAME:
            s3.Bucket(bucket_name).download_file(i, pdf_path+i.replace('rss/pdf/2019-01-24/', ""))
    except Exception as e:
        print(e)
        pass 

def uplaod_s3(bucket_name_ML):
    
    pass


if __name__ == "__main__":
    # mkdir data dir
    bucket_name = 'researchkernel-datalake'
    bucket_name_ML = 'researchkernel-machinelearning'
    try:
        download_s3_pdf(bucket_name)
        download_rss_pdf(bucket_name)
    except:
        pass

    filenames = pdf_dir()
    filenames = filenames
    try:
        pool = multiprocessing.Pool()
        pool.map(pdf_extract, filenames)
        pool.close()
        pool.join()
    except:
        pass
    
