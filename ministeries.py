from db_conn import DatabaseConnection
import csv
from datetime import datetime

db_cursor = DatabaseConnection()


data = [row for row in csv.DictReader(open("ministries.csv"))]
new_data = [row for row in csv.DictReader(open("new_ministries.csv"))]

class Ministry():
    
    
    def change_project_ministries(self):
        
        """This function changes the ministry id in a project. We are given a unique list of ministries and their IDs. We check for the ministry id attached to a project, then compare if it is in the unique list of ids, then it updates it with the respective ministry id that matches it from the unique list.
        """

        cur = db_cursor.get_cursor()
        cur.execute('SELECT * FROM "Projects"')
        projects = cur.fetchall()
        get_min_query = 'SELECT * FROM "Ministries" WHERE id=%s'
        update_proj_min = 'UPDATE "Projects" SET "ministryId"=%s'
        count = 0
        count_proj =0 
        
        for project in projects:
            # Ministry.data['id']
            min_id = project['ministryId']
            cur.execute(get_min_query, (min_id,)) #get the ministry from db
            min = cur.fetchone()
            count_proj+=1
            # print(project['name'])
            
            for row in data:
                if min['name'] == row['name']: #check if the ministry name is equal to a name in our unique ministry then set the id of the unique ministry to the current project with the same name
                    
                    cur.execute(update_proj_min, (int(row['id']),)) #run the update query
                    
                    DatabaseConnection.connect_.commit() #save the update
                    
                    print(f"{min['name']}, {row['name']}")
                    print(f"Changed {min_id} to {row['id']}")
                    print('==============')
                    print()
                    count+=1
                    break
                    
                # print(row)
        print(count) 
        print(count_proj) 
              
    def delete_duplicate_ministries(self):
        
        """This function checks all the data in the database, if the id of that ministry is not in the unique list of ids, we delete it."""
        
        cur = db_cursor.get_cursor()
        get_mins = 'SELECT * FROM "Ministries"'
        cur.execute(get_mins)
        ministries = cur.fetchall()
        mins_ids = [int(row['id']) for row in data]
        # print(mins_ids)
        delete_duplicate = 'DELETE FROM "Ministries" WHERE "id"=%s'
        
        for min in ministries:
            # print(min)
            if min['id'] not in mins_ids:
                cur.execute(delete_duplicate, (min['id'],)) #delete the duplicate
                DatabaseConnection.connect_.commit() #save
                print(min['id'])
                
                
            
        return
                
                
    def create_new_ministries(self):
        """This function is used to create new ministries from the new_data that was provided."""
        
        cur = db_cursor.get_cursor()
        create_mins = """INSERT INTO "Ministries" ("name", "countryId", "deleted", "createdAt", "updatedAt" ) VALUES (%s, 160, %s, %s, %s)"""
        
        for data in new_data:
            print(data)
            cur.execute(create_mins, (data['name'], False, datetime.now(), datetime.now()))
            DatabaseConnection.connect_.commit() 
            print(data['name'])
            print("===========")
            print()
            
        # ministries = cur.fetchall()
       

        

    
    

ministry = Ministry()

ministry.change_project_ministries()
ministry.delete_duplicate_ministries()
ministry.create_new_ministries()

# DatabaseConnection.close_connection()
# print(projects)