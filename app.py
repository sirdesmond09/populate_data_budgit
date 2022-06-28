from db_conn import DatabaseConnection
from flask import Flask, jsonify, request
from flask_restful import Api, Resource
from flask_cors import CORS

app = Flask(__name__)
CORS(app)
api = Api(app)

db_cursor = DatabaseConnection()

class ProjectApi(Resource):
    def get(self):
        
        cur = db_cursor.get_cursor()
        cur.execute('SELECT * FROM "Projects" WHERE state=%s and year=%s', ("Lagos","2021"))
        projects = cur.fetchmany(5)
        db_cursor.close_connection()
        
        data = {"message":"success",
                "data":projects}
        return jsonify(data)
    
    
    def post(self):
        f = request.files['file']
        


api.add_resource(ProjectApi,'/projects')


if __name__=='__main__':
    app.run(debug=True)