from db_conn import DatabaseConnection
import csv
from datetime import datetime

db_cursor = DatabaseConnection()


data = [row for row in csv.DictReader(open("locations.csv"))]

class Location():
    
    
    def change_project_locations(self):
        
        """This function changes the location id in a project. We are given a unique list of locations and their IDs. We check for the location id attached to a project, then compare if it is in the unique list of ids, then it updates it with the respective location id that matches it from the unique list.
        """

        cur = db_cursor.get_cursor()
        cur.execute('SELECT * FROM "Projects"')
        projects = cur.fetchall()
        
        get_loc_query = 'SELECT * FROM "Locations" WHERE id=%s'
        update_proj_loc = 'UPDATE "Projects" SET "locationId"=%s'
       
        count = 0
        count_proj =0 
        for project in projects:
            
            loc_id = project['locationId']
            cur.execute(get_loc_query, (loc_id,)) #get the location from db
            loc = cur.fetchone()
            count_proj +=1
            # print(project['name'])
            for row in data:
                # print(loc['name'])
                if loc['name'].lower() == row['name'].lower(): #check if the location name is equal to a name in our unique locations then set the id of the unique location to the current project with the same location name.
                    
                    cur.execute(update_proj_loc, (int(row['id']),)) #run the update query
                    
                    DatabaseConnection.connect_.commit() #save the update
                    
                    print(f"{loc['name']}, {row['name']}")
                    print(f"Changed {loc_id} to {row['id']}")
                    print('==============')
                    print()
                    count+=1
                    break
                
        print("\n", count)  
        print("\n", count_proj)         
                # print(row)
         
              
    def delete_duplicate_locations(self):
        
        """This function checks all the data in the database, if the id of that location is not in the unique list of ids, we delete it."""
        
        cur = db_cursor.get_cursor()
        get_locs = 'SELECT * FROM "Locations"'
        cur.execute(get_locs)
        locations = cur.fetchall()
        loc_id = [int(row['id']) for row in data]
        # print(loc_id)
        delete_duplicate = 'DELETE FROM "Locations" WHERE "id"=%s'
        
        for location in locations:
            # print(location)
            if location['id'] not in loc_id:
                cur.execute(delete_duplicate, (location['id'],)) #delete the duplicate
                DatabaseConnection.connect_.commit() #save
                print(location['id'])
                
                
            
        return
                
                
    
       

        

    



loc = Location()

loc.change_project_locations()
# loc.delete_duplicate_locations()
