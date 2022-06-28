from db_conn import DatabaseConnection
import csv
from datetime import datetime

#change the name length to 2500 to accomodate data on the projects table

db_cursor = DatabaseConnection()


data = [row for row in csv.DictReader(open("projects.csv"))]


class Projects():
    
    
    def update_csv(self):
        former_ids = [x for x in range(548, 557)]
        new_ids = [x for x in range(547, 555)]
        
        
        for data_ in data:
            if int(data_['ministryId']) >538:
                for index in range(len(former_ids)):
                    if int(data_['ministryId']) == former_ids[index]:
                        print(data_['ministryId'], former_ids[index])
                        print(data_['ministryId'], new_ids[index])
                        print("===================")
            
        else:
            with open("updated_proj1.csv", "w", encoding='UTF8', newline='')  as file:  
                writer = csv.DictWriter(file, fieldnames=data[0].keys())
                writer.writeheader()
                writer.writerows(data)
    
    
    def create_new_projects(self):
        """This function is used to create new ministries from the new_data that was provided."""
        
        new_data = [row for row in csv.DictReader(open("updated_proj1.csv"))]
        count=0
        cur = db_cursor.get_cursor()
        create_projs = 'INSERT INTO "Projects" ("name", "description", "amount","source_link","year","num_views","upvote","downvote","state","amount_disbursed","address","deleted","is_active","yearId","countryId","locationId","sectorId","areaId","agencyId","ministryId","userId","createdAt","updatedAt","originalId","meta","stakeholderMobile","stakeholderEmail","slug","region","status","stakeholderName","longitude","latitude") VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
        
        
        for data in new_data:
            data["deleted"] =False
            data['is_active'] = True
            data['sectorId'] = None
            data['areaId'] = None
            data['agencyId'] = None
            data['userId'] = None
            
            
            cur.execute(create_projs,  tuple(data.values()))
            DatabaseConnection.connect_.commit() 
            print(data['name'])
            count+=1
            print("===========")
            print()
              
    
        print(count)
        return  
                
    
       

        

    
    

test = Projects()

# test.update_csv()
test.create_new_projects()
# test.delete_duplicate_ministries()

# print(projects)