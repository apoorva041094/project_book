import importlib
from flask import Flask, request
from flask_restful import Resource, Api
from response import Response

app = Flask(__name__)
api = Api(app)

class BookData(Resource):
    def get(self):
        run_process = load_module("books").list(request)
        count =  len(run_process)
        return Response(201, run_process, count).prepare_http_response()
        
class BookTest(Resource):
    def get(self):
        return "successfull"
        
api.add_resource(BookData, '/api/list')
api.add_resource(BookTest, '/api/test')


def load_module(module_name):
    module = importlib.import_module(module_name)
    provider_class = getattr(module, module_name.title())()
    return provider_class


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000, debug=True) 