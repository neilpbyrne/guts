from textblob import TextBlob
from gutenberg.acquire import load_etext
from gutenberg.cleanup import strip_headers
from gutenberg.query import get_etexts
from gutenberg.query import get_metadata
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import pandas as pd
import re
# ====== Connection ====== #
# Connection to ElasticSearch
es = Elasticsearch(['http://localhost:9220'],timeout=600)
# Simple index creation with no particular mapping
# es.indices.create(index='books',body={})
from gutenberg.query import list_supported_metadatas

print(list_supported_metadatas()) # prints (u'author', u'formaturi', u'language', ...)

cols = ['id', 'text', 'author', 'title', 'language', 'subject', 'textsummary']
# cols = ['id', 'text', 'author', 'title', 'language']

# cols = ['id', 'text']


dat = pd.DataFrame(columns = cols)
i = 0
for i in range(500, 1500):
  try:
    text = strip_headers(load_etext(i+1)).strip()
    author = str(get_metadata('author', i+1)).partition("[u'")[2].partition("'])")[0]
    title = str(get_metadata('title', i+1)).partition("[u'")[2].partition("'])")[0]
    language = str(get_metadata('language', i+1)).partition("[u'")[2].partition("'])")[0]
    subject = str(get_metadata('subject', i+1))

    subjects = subject.split(',')

    cleansubjects = []

    textsummary = TextBlob(text)

    entities =[]

    for element in textsummary.tags:
      if str(element[1]).find('NNP') >= 0 and element[0] not in entities and len(str(element[0])) > 3:
        entities.append(element[0])

    for subject in subjects:
      clean = subject.partition("[u'")[2].partition("'])")[0]
      if len(clean) > 0:
        cleansubjects.append(clean.replace("'",""))


    # print(subject)
    # dat = dat.append({'id': i+1, 'text':text},ignore_index=True)

    dat = dat.append({'id': i+1, 'text':text, 'author': author, 'title': title, 'language':language, 'subject': cleansubjects, 'textsummary':entities},ignore_index=True)
    print ("grabbed book" + str(i))
  except:
    print("missed book " + str(i))
  if i% 10 == 0:
    books = dat.to_dict(orient='records')
    bulk(es, books, index='books_enhanced_store_fd', doc_type='doc', raise_on_error=True)
    dat = pd.DataFrame(columns=cols)
    print ("elastic dump @ " + str(i))



# print(get_metadata('title', 2701))  # prints frozenset([u'Moby Dick; Or, The Whale'])
# print(get_metadata('author', 2701)) # prints frozenset([u'Melville, Hermann'])
#
# print(get_etexts('title', 'Moby Dick; Or, The Whale'))  # prints frozenset([2701, ...])
# print(get_etexts('author', 'Melville, Hermann'))        # prints frozenset([2701, ...])
