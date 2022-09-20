from abc import ABCMeta
from itertools import count
import json


class Response(metaclass=ABCMeta):
    def __init__(self, code, data, count):
        self.__code = code
        self.__data = data
        self.__count = count
        self.__body = None
        self.__outgoing_response = None

    @property
    def code(self):
        """
        http status code
        :return: status code
        """
        return self.__code
    
    @property
    def count(self):
        """
        http status code
        :return: status code
        """
        return self.__count

    @property
    def data(self):
        """
        data for sending response
        :return: data
        """
        return self.__data

    @property
    def body(self):
        """
        prepared http body
        :return: HTTP Response Body
        """
        return self.__body

    @body.setter
    def body(self, value):
        """
        prepared http body
        :param value: prepared http body
        :return: None
        """
        self.__body = value

    def prepare_http_response(self):
        """
        All HTTP client will be sent from this method.
        It will also log client response in DB
        :return: HTTP Response
        """
        if self.__code in [200, 201]:
            self.body = {"status": "success", "code": self.code, "count": self.count ,"data": self.data}
        else:
            self.body = {"status": "failed", "code": self.code, "errors": self.data}

        response_json = {'status': self.code, 'body': self.body}
        return self.body, self.code
