import os
import boto3
import feedparser
import requests
import urllib.request
import datetime
import re
import itertools
import pandas as pd

# Function for parsing arxiv open API Feed. 

def extract_metadata(feed):
    '''
                Function: Extract all metadata from arxiv respose

                Input: takes api respose from arxiv, arxiv_id in our database

                Return: list of dictionaries
    '''
    # for time formating
    f = '%Y-%m-%d %H:%M:%S'
    now = datetime.datetime.now()
    created_at = now.strftime(f)
    updated_at = now.strftime(f)

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
        metadata_dict['created_at'] = created_at
        metadata_dict['updated_at'] = updated_at
        metadata_dict_list.append(metadata_dict)

    return metadata_dict_list

# Function for downloading recently publised paper's Metadata and pdf links. 

def download(bucket_name, s3_text_filename, s3_json_filename, local_json_path, local_text_path):
    
    s3 = boto3.client('s3')

    apis = [
        'astro-ph','cond-mat', 'cs', 'econ', 'eess', 'gr-qc', 'hep-ex', 'hep-lat',
        'hep-ph', 'hep-th', 'math', 'math-ph', 'nlin', 'nucl-ex', 'nucl-th',
        'physics', 'q-bio', 'q-fin', 'quant-ph', 'stat'
        ]
    
    arr = []

    # getting papers for each api sequentially 
    # No point to use multiprocessing    
    try:
        for api in apis:
            response = requests.get("http://export.arxiv.org/rss/" + api)
            items = str(response.text).replace('\n', '')
            m = re.search('<rdf:Seq>(.+?)</rdf:Seq>', items)
            print('doing for ' + api)
            a = re.split('"', m.group(1))
            a = a[1::2]
            for i in range(len(a)):
                a[i] = a[i].replace('http://arxiv.org/abs/', '')
            arr.extend(a)
            print('done for {} got {} records'.format(api, len(a)))
            print('found total {} records'.format(len(arr)))
            f2 = open(local_text_path, 'w')
            f2.write(str(arr))
            base_url = 'http://export.arxiv.org/api/query?search_query='
            urls = ['http://export.arxiv.org/api/query?search_query={0}'.format(str(element)) for element in arr]
            data = []

        # getting response of earch url and append that to a list. 
        for url in urls:
            try:
                response = urllib.request.urlopen(url).read()
                response = response.decode('utf-8')
                feed = feedparser.parse(response)
                parsed_data = extract_metadata(feed)
                data.append(parsed_data)
            except Exception as e:
                pass
        # making dataframe from that list and converting list to json.
        try:
            data = list(itertools.chain.from_iterable(data))
            data = pd.DataFrame(data)
            data.T.to_json(str(datetime.date.today())+".json")
        except Exception as e:
            print(e)
            pass
    except Exception as e:
        print("arxiv rss service is down !!! on "+ str(datetime.date.today()))
        pass
    # sending data to datalake for saving 
    try:
        s3.upload_file(local_text_path, bucket_name, s3_text_filename)
        print("Saved txt file to S3 on "+ str(datetime.date.today()))
        s3.upload_file(local_json_path, bucket_name, s3_json_filename)
        print("Saved csv file to S3 on "+ str(datetime.date.today()))
    except Exception as e:
        print(e)
        pass


if __name__ == "__main__":
    bucket_name = 'researchkernel-datalake'
    s3_text_filename = 'rss/txt/' + str(datetime.date.today()) + '.txt'
    s3_json_filename = 'rss/json/' + str(datetime.date.today()) + '.json'
    local_text_path = str(datetime.date.today()) + '.txt'
    local_json_path = str(datetime.date.today()) + '.json'
    download(bucket_name, s3_text_filename, s3_json_filename, local_json_path, local_text_path)
    