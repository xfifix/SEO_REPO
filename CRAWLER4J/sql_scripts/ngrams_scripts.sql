CREATE TABLE IF NOT EXISTS NGRAMS_WORDS(
    NGRAMS TEXT UNIQUE NOT NULL,
    NB_DOCUMENTS INT,
    DOC_LIST TEXT,
    LAST_UPDATE DATE
) TABLESPACE mydbspace;

CREATE TABLE IF NOT EXISTS NGRAMS_WORDS_METADATA(
    THEMA TEXT,
    NB_TOTAL_DOCUMENTS INT
) TABLESPACE mydbspace;


CREATE INDEX ON NGRAMS_WORDS(NGRAMS);
# the table which will contain the number of occurences (document containing the specified word)
# we've found in our documents corpus
# SQL requests from the CorpusDataController

# we can't practise the upsert methodology : to update the row, we need the former row data
# we try to find the word, if not present, we insert a row, else we update a row
select * from NGRAMS_WORDS where NGRAMS='****'

if not found, we insert a row with the word, the url and the number of documents to one
# insert statement 
INSERT INTO NGRAMS_WORDS(NGRAMS,NB_DOCUMENTS,DOC_LIST) values(?,?,?)

INSERT INTO NGRAMS_WORDS_METADATA(THEMA,NB_TOTAL_DOCUMENTS) values('TOTAL_NUMBER_DOCUMENTS',1000000)

# UPDATE
UPDATE CORPUS_WORDS_METADATA SET NB_TOTAL_DOCUMENTS=? WHERE THEMA='TOTAL_NUMBER_DOCUMENTS'


# if found, we check if the document is not already present in our list
# if the document is already present we do nothing
# if the document is not present we just update the row by incrementing nb_documents and appending the current URL to the documents
UPDATE NGRAMS_WORDS WORD=?,NB_DOCUMENTS=?,DOC_LIST=? WHERE NGRAMS=?";
