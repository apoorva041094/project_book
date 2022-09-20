from db_func import DB
class Books:
    def __init__(self):        
        self.page = 0

    def list(self, request):


        if request.args.get('page'):
            self.page = request.headers.get('page_no')
        books_data = DB().getBooksData(self.page, request)
        return books_data
    

    # def __del__(self):
    #     DB().drop_all_views(self.db_engine)
