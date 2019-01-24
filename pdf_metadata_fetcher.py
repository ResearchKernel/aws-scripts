import datetime
import feedparser
import pandas as pd
import feedparser
import os
import time
import re
import urllib.request
import requests
import boto3
import itertools

def extract_metadata(feed):
    '''
                Function: Extract all metadata from arxiv respose

                Input: takes api respose from arxiv, arxiv_id in our database

                Return: list of dictionaries
    '''
    global db_arxiv_id

    metadata_dict_list = []  # save dicts
    for entry in feed.entries:
        metadata_dict = {}  # save metadata respose into dict.
        arxiv_id = entry.id.split('/abs/')[-1]
        print("fetched id:", arxiv_id)
        published = entry.published
        title = entry.title
        author_string = entry.author
        try:
            author_string += ' (%s)' % entry.arxiv_affiliation
        except AttributeError:
            pass
        last_author = author_string

        # feedparser v5.0.1 correctly handles multiple authors, print them all
        try:
            Authors = (', ').join(author.name for author in entry.authors)
        except AttributeError:
            pass
            # get the links to the abs page and pdf for this e-print
        for link in entry.links:
            if link.rel == 'alternate':
                abs_page_link = link.href
                print(abs_page_link)
            elif link.title == 'pdf':
                # The journal reference, comments and primary_category sections live under # the arxiv namespace
                pdf_link = link.href
        try:
            journal_ref = entry.arxiv_journal_ref
        except AttributeError:
            journal_ref = 'No journal ref found'
        try:
            comment = entry.arxiv_comment
        except AttributeError:
            comment = 'No comment found'
        primary_category = entry.tags[0]['term']
        # Lets get all the categories
        all_cat = [t['term'] for t in entry.tags]
        all_categories = (', ').join(all_cat)
        # The abstract is in the <summary> element
        Abstract = entry.summary
        metadata_dict['arxiv_id'] = arxiv_id
        metadata_dict['title'] = title
        metadata_dict['abstract'] = Abstract
        metadata_dict['primary_category'] = primary_category
        metadata_dict['all_categories'] = all_categories
        metadata_dict['author'] = author_string
        metadata_dict['last_author'] = last_author
        metadata_dict['authors'] = Authors
        metadata_dict['published'] = published
        metadata_dict['journal_ref'] = journal_ref
        metadata_dict['comment'] = comment
        metadata_dict['abs_page_link'] = abs_page_link
        metadata_dict['pdf_link'] = pdf_link
        metadata_dict_list.append(metadata_dict)
    return metadata_dict_list

def download_metadata_pdf(path, bucket_name, json_filename,arxiv_id):

    urls = [
        'http://export.arxiv.org/api/query?search_query={0}'.format(str(element)) for element in arxiv_id]

    data = []
    for url in urls:
        response = urllib.request.urlopen(url).read()
        response = response.decode('utf-8')
        feed = feedparser.parse(response)
        data.append(extract_metadata(feed))
    try:
        data = list(itertools.chain.from_iterable(data))
        data = pd.DataFrame(data)
        data.T.to_json(str(datetime.date.today())+"_pdf.json")
    except Exception as e:
        print(e)
    try:
        s3.upload_file(path, bucket_name, json_filename)
    except Exception as e:
        pass



def get_tarfile_link(bucket_name):
    s3 = boto3.client('s3')
    TAR_FILENAME = []
    PREFIX = 's3_pdf/pdf/'
    result = s3.list_objects(Bucket=bucket_name,
                            Prefix=PREFIX,
                            Delimiter='/')
    try:
        for i in range(1,5):
            TAR_FILENAME.append(result["Contents"][i]["Key"])
    except Exception as identifier:
        pass
    return TAR_FILENAME

def upload_pdf(bucket_name):
    s3 = boto3.client('s3')
    os.system('zip -r '+str(datetime.date.today())+'.zip ./data/pdf/')
    


if __name__ == "__main__":
    s3 = boto3.client('s3')
    PDF_DIR = "./data/"
    S3_filename = 's3_pdf/json/'+str(datetime.date.today()) + '_pdf.json'
    json_path = "./data/"+str(datetime.date.today()) + '_pdf.json'
    bucket_name = 'researchkernel-datalake'

    # Download
    try: 
        os.mkdir("./data/")
        for i in links:
            s3.download_file(bucket_name, i, i)
            os.system("tar xzf "+i+" -C data/")
            s3.delete_object(Bucket=bucket_name, Key=i)
    except Exception as e:
        pass
    
    # list of filenames
    filenames_base_list = []

    for (dirpath, dirnames, filenames) in os.walk("./data/"):
            filenames_base_list += [os.path.join(dirpath, file) for file in filenames]
    arxiv_id_filenames_base = [os.path.basename(i) for i in filenames_base_list[1:]]
    arxiv_id = [os.path.splitext(i)[0] for i in arxiv_id_filenames_base]
    
    # Links of TARFILE from S3
    links = get_tarfile_link(bucket_name)

    #Download metadata of PDF 
    download_metadata_pdf(json_path, bucket_name, S3_filename,arxiv_id)

    



