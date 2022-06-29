from .db_conn import DatabaseConnection
from flask import Flask, jsonify, request
from flask_restful import Api, Resource, abort
from flask_cors import CORS

app = Flask(__name__)
CORS(app)
api = Api(app)

db_cursor = DatabaseConnection()

def get_paginated_list(results, url, start, limit):
    start = int(start)
    limit = int(limit)
    count = len(results)
    if count < start or limit < 0:
        abort(404)
    # make response
    obj = {}
    obj['start'] = start
    obj['limit'] = limit
    obj['count'] = count
    # make URLs
    # make previous url
    if start == 1:
        obj['previous'] = ''
    else:
        start_copy = max(1, start - limit)
        limit_copy = start - 1
        obj['previous'] = url + '?start=%d&limit=%d' % (start_copy, limit_copy)
    # make next url
    if start + limit > count:
        obj['next'] = ''
    else:
        start_copy = start + limit
        obj['next'] = url + '?start=%d&limit=%d' % (start_copy, limit)
    # finally extract result according to bounds
    obj['results'] = results[(start - 1):(start - 1 + limit)]
    return obj

def sort_results(data, key:str):
    """Function to sort the data in the array by a given key."""
    if key.startswith("-"):
        print(key)
        key = key.replace("-", "")
        print(key)
        return sorted(data, key=lambda x: x[key], reverse=True)
   
    return sorted(data,key=lambda x: x[key])



class ProjectApi(Resource):
    def get(self):
        
        args = request.args
        state = args.get('state')
        year = args.get('year')
        orderBy = args.get('orderBy')
        search = args.get('search')
        # print(state, year)
        
        cur = db_cursor.get_cursor()
        cur.execute('SELECT * FROM "Projects"')
        
        if state is not None:
            
            cur.execute('SELECT * FROM "Projects" WHERE state=%s or state=%s', (state.lower(), state.title()))
        
        if year is not None:
            cur.execute('SELECT * FROM "Projects" WHERE year=%s', (year,))
            
        if year and state:
            cur.execute('SELECT * FROM "Projects" WHERE (state=%s or state=%s) and year=%s', (state.lower(), state.title(), year))
        
            
        
        projects = cur.fetchall()
        db_cursor.close_connection()
        
        if orderBy is not None:
            projects = sort_results(projects, orderBy)
        
        data = {"message":"success",
                "data":get_paginated_list(
        projects, 
        '/projects', 
        start=request.args.get('start', 1), 
        limit=request.args.get('limit', 6)
    )}
        
        
        return jsonify(data=data)
    
        


api.add_resource(ProjectApi,'/projects')


