from email import header
from sqlite3 import Cursor
from urllib import request
from constants import db_cred
import pandas as pd
import json
import mysql.connector

class DB:
    
    def __init__(self):
        self.conn = mysql.connector.connect(host=db_cred['host'],
                                                user=db_cred['user'],
                                                passwd=db_cred['password'],
                                                database = db_cred['database'] )
        
        self.cursor = self.conn.cursor(buffered=True)
        # return self.cursor
        exists = self.check_view_exists()
        if(exists == True):
            self.create_view_author()
            self.create_view_shelf()
            self.create_view_subject()
            self.create_view_language()

    def check_view_exists(self):
        query = "SHOW FULL TABLES IN book_data WHERE TABLE_TYPE LIKE 'VIEW';"
        self.cursor.execute(query)
        tables_list = self.cursor.fetchall()
        if len(tables_list) > 0 :
            return False
        else:
            return True



    def create_view_author(self):
        query = "CREATE VIEW view_author AS\
                    SELECT  T1.id as 'book_id', T1.title, GROUP_CONCAT(T3.Name ORDER BY T3.Name) as 'author' FROM books_book T1\
                    INNER JOIN books_book_authors T2 ON T1.id = T2.book_id\
                    INNER JOIn books_author T3 ON T2.author_id = T3.id\
                    GROUP BY T1.id, T1.title;"
        self.cursor.execute(query)

    def create_view_shelf(self):
        query = "CREATE VIEW view_shelf AS \
                    SELECT  T1.id as 'book_id', T1.title 'title', GROUP_CONCAT(T3.Name ORDER BY T3.Name) as 'shelf' \
                    FROM books_book T1 \
                    INNER JOIN books_book_bookshelves T2 ON T1.id = T2.book_id \
                    INNER JOIn books_bookshelf T3 ON T2.bookshelf_id = T3.id \
                    GROUP   BY T1.id, T1.title;"
        self.cursor.execute(query)
        # pd.read_sql_query(query, con=engine)

    
    def create_view_subject(self):
        query = "CREATE VIEW view_subject AS \
                SELECT  T1.id as 'book_id', T1.title, GROUP_CONCAT(T3.Name ORDER BY T3.Name) as 'subject' FROM books_book T1 \
                INNER JOIN books_book_subjects T2 ON T1.id = T2.book_id \
                INNER JOIn books_subject T3 ON T2.subject_id = T3.id \
                GROUP BY T1.id, T1.title;"
        self.cursor.execute(query)
        # return pd.read_sql(query, con=engine)


    def create_view_language(self):
        query = "CREATE VIEW view_language AS \
                    SELECT  T1.id as 'book_id',  T1.title, GROUP_CONCAT(T3.code ) as 'language_code' FROM books_book T1\
                    INNER JOIN books_book_languages T2 ON T1.id = T2.book_id \
                    INNER JOIn books_language T3 ON T2.language_id = T3.id \
                    GROUP BY T1.id, T1.title;"
        self.cursor.execute(query)

    
    
    
    
    def getBooksData(self, page, request):
        start_flag = 0
        if page != 0:
            start_flag = int(page) * 25

        self.author = '%'
        self.language = '%'
        self.title = '%'
        self.mime_type = '%'
        self.book_id = ''
        self.topic = ''
        if request.args.get('author'):
            self.author = request.args.get('author')

        if request.args.get('language'):
            self.language = request.args.get('language')
        
        if request.args.get('title'):
            self.title = request.args.get('title')
        
        if request.args.get('book_id'):
            self.book_id = request.args.get('book_id')
        
        if request.args.get('mime_type'):
            self.mime_type = request.args.get('mime_type')
        
        if request.args.get('topic'):
            self.topic = request.args.get('topic')
        
        
        bookData = self.getBookIdData(start_flag, self.author, self.language , self.title, self.book_id, self.mime_type, self.topic)
        
        book_df = pd.DataFrame(bookData, columns=[
                                                'book_id',
                                                'title',
                                                'author',
                                                'shelf',
                                                'subject',
                                                'language_code',
                                                'url',
                                                'mime_type'])
        print(book_df)
        book_df = book_df.to_json(orient='records')
        book_df = json.loads(book_df)
        return book_df


    def getBookIdData(self, start_flag, author, language, title, book_id, mime_type, topic):
        book_filter = ''
        if book_id:
            book_filter = "and books_book.id IN ("+ book_id +")"
        query = "SELECT books_book.id, \
                    books_book.title,\
                    view_author.author, \
                    view_shelf.shelf ,\
                    view_subject.subject,\
                    view_language.language_code,\
                    books_format.url, books_format.mime_type\
                    FROM books_book\
                    join view_author on view_author.book_id = books_book.id\
                    join view_shelf on view_shelf.book_id = books_book.id\
                    join view_subject on view_subject.book_id = books_book.id\
                    join view_language on view_language.book_id = books_book.id\
                    join books_format on books_format.book_id = books_book.id\
                        where view_author.author like '"+ author +"' \
                        and view_language.language_code like '"+ language +"' \
                        and books_format.mime_type like '"+ mime_type +"' \
                        and books_book.title like '"+ title + "' \
                        and (view_subject.subject like '%"+ topic +"%' OR view_shelf.shelf like '%"+ topic +"%')\
                        "+ book_filter +" \
                    order by books_book.download_count desc \
                    limit "+ str(start_flag) + ",25;"
        
        self.cursor.execute(query)
        data = self.cursor.fetchall()
        return data






    # def getAuthorData(self, engine, start_flag, filter_keyword='%'):
    #     query = "select books_book_authors.book_id, books_author.name as author_name from books_author join books_book_authors on  books_book_authors.author_id = books_author.id where books_author.name like '"+ filter_keyword +"' ;"
    #     data = pd.read_sql(query, con=engine)
    #     return data 
    
    # def getShelfData(self, engine, start_flag, filter_keyword='%'):
    #     query = "SELECT books_book_bookshelves.book_id, books_bookshelf.name as bookshelf_name FROM books_bookshelf join books_book_bookshelves ON books_book_bookshelves.bookshelf_id = books_bookshelf.id where books_bookshelf.name = '"+ filter_keyword +"';"
    #     data = pd.read_sql(query, con=engine)
    #     return data 

    
        
        
    



    def drop_all_views(self, engine):
        query = "drop view view_author, view_shelf, view_subject, view_language;"
        return pd.read_sql(query, con = engine)



