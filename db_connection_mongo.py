#-------------------------------------------------------------------------
# AUTHOR: Alexander Okonkwo
# FILENAME: index_mongo.py
# SPECIFICATION: Provides methods for interacting with mongoDB database
# FOR: CS 4250- Assignment #2
# TIME SPENT: 2 hours
#-----------------------------------------------------------*/
from pymongo import MongoClient
from datetime import datetime

def connectDataBase():
    client = MongoClient("mongodb+srv://owwix:Nsxlover32!@cluster0.n1zt8.mongodb.net/")
    db = client['A2']
    collection = db['A2'] 

def createDocument(col, docId, docText, docTitle, docDate, docCat):
    terms = docText.lower().split(" ")
    term_counts = {}
    for term in terms:
        if term not in term_counts:
            term_counts[term] = 1
        else:
            term_counts[term] += 1
    
    # Create a dictionary (document) to count how many times each term appears in the document
    term_list = [{"term": term, "count": count, "num_chars": len(term)} for term, count in term_counts.items()]

    # Producing a final document as a dictionary including all the required fields
    document = {
        "id": docId,
        "text": docText,
        "title": docTitle,
        "date": docDate,
        "category": docCat,
        "terms": term_list
    }
    
    # Insert the document
    col.insert_one(document)

def deleteDocument(col, docId):
    # Delete the document from the database
    col.delete_one({"id": docId})

# Update an existing document in the database
def updateDocument(col, docId, docText, docTitle, docDate, docCat):
    # Delete the document
    deleteDocument(col, docId)
    
    # Create the document with the same ID
    createDocument(col, docId, docText, docTitle, docDate, docCat)

# Produce an inverted index where each term maps to the documents it appears in, with occurrence counts
def getIndex(col):
    # Query the database to return the documents where each term occurs with their corresponding count.
    all_docs = col.find()
    inverted_index = {}
    
    for doc in all_docs:
        title = doc['title']
        for term_info in doc['terms']:
            term = term_info['term']
            count = term_info['count']
            
            if term not in inverted_index:
                inverted_index[term] = {}
            
            if title not in inverted_index[term]:
                inverted_index[term][title] = count
            else:
                inverted_index[term][title] += count
    
    formatted_index = {}
    for term, occurrences in inverted_index.items():
        formatted_index[term] = ",".join([f"{doc_title}:{count}" for doc_title, count in occurrences.items()])
    
    return formatted_index