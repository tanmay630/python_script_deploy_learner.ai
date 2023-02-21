
import requests
import json
import random
import time
from datetime import datetime
import schedule
import psycopg2

class ContactUpdater:
    def __init__(self,client_id, client_secret, refresh_token, server_name=None, database_name=None, username=None, password=None):
        self.server_name = server_name
        self.database_name = database_name
        self.username = username
        self.password = password
        self.client_id = client_id
        self.client_secret = client_secret
        self.refresh_token = refresh_token
        self.headers = None
        self.access_token = None
        self.conn = None
        self.cursor = None
        self.create_connection()

     # create a connection to the database
    def create_connection(self):
        con = psycopg2.connect("postgres://hnjpghwq:qkP_muDD20-oakglSE18dbQ7XNvUHrEq@mouse.db.elephantsql.com/hnjpghwq")
        self.conn = con
        self.cursor = self.conn.cursor()
     # ContactTable class represents the contact table in the database
    def create_table(self):
        self.cursor.execute('''CREATE TABLE IF NOT EXISTS contact (
        id SERIAL PRIMARY KEY,
        first_name VARCHAR(50) NOT NULL,
        last_name VARCHAR(50) NOT NULL,
        email VARCHAR(50) NOT NULL,
        hubspot_id INT,
        create_date TIMESTAMP NOT NULL
        )
        ''')
        # insert random data in the database
    def insert_data(self):
        for i in range(2):
            first_name = f"FirstName{random.randint(1,100)}"
            last_name = f"LastName{random.randint(1,100)}"
            email = f"{first_name.lower()}.{last_name.lower()}@example.com"
            create_date = datetime.now()
            self.cursor.execute("INSERT INTO contact (first_name, last_name, email, create_date) VALUES (%s, %s, %s, %s)", (first_name, last_name, email, create_date))
        self.conn.commit()
     # fetch data from the database
    def fetch_data(self):
        self.cursor.execute("""SELECT * FROM "public"."contact" LIMIT 100""")
        rows = self.cursor.fetchall()
        return rows
    # get access token from hubspot API credentials
    def get_access_token(self):
        return self.refresh_token
        url = f"https://api.hubapi.com/oauth/v1/token"
        headers = {
            "Content-Type": "application/x-www-form-urlencoded;charset=utf-8",
            "Authorization": "Bearer {}".format(self.refresh_token)

        }
        data = {
            "grant_type": "refresh_token",
            # "client_id": self.client_id,
            # "client_secret": self.client_secret,
            # "refresh_token": self.refresh_token
        }
        response = requests.post(url, headers=headers, data=data)
        if response.status_code == 200:
            data = json.loads(response.content)
            self.access_token = data["access_token"]
            self.headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self.access_token}"
            }
           
            print ("Access token updated successfully!")
        else:
            print(response.json())
            print("Failed to update access token.")
     # Update the records in SQL Database with HubSpot Id that you get in response.
    def update_contact(self, contact):
        url = f"https://api.hubapi.com/contacts/v1/contact/createOrUpdate/email/{contact[3]}"
        data = {
            "properties": [
                {
                    "property": "firstname",
                    "value": contact[1]
                },
                {
                    "property": "lastname",
                    "value": contact[2]
                },
                {
                    "property": "email",
                    "value": contact[3]
                }
            ]
        }
        print("send request")
        headers = {           
                         'Content-Type': 'application/json',
                   "Authorization": "Bearer {}".format(self.refresh_token)

        }
        response = requests.post(url, headers=headers, data=json.dumps(data))
        print(response.status_code)
        print(response.text)
        if response.status_code == 200:
            data = json.loads(response.content)
            if "vid" in data:
                hubspot_id = data["vid"]
                self.cursor.execute(f"UPDATE contact SET hubspot_id = {hubspot_id} WHERE email = '{contact[3]}'")
                self.conn.commit()
                print(f"Contact {contact[1]} {contact[2]} ({contact[3]}) updated successfully!")
            else:
                print(f"Failed to update contact {contact[1]} {contact[2]} ({contact[3]}).")
        else:
            print(f"Failed to update contact {contact[1]} {contact[2]} ({contact[3]}).")

    def update_records(self):
        print("update_records")
        rows = self.fetch_data()
        print("len", len(rows))
        for row in rows:
            self.update_contact(row)


# Rest of the code here...

if __name__ == "__main__":
    # Create an instance of the ContactUpdater class
    cu = ContactUpdater(client_id="d984097d-7fd1-4a8c-b39a-51c1bbcbe740", client_secret="eb596ae8-96ff-40b5-925c-4284b68dd1e9", refresh_token="pat-na1-fd3af4fc-cb8b-421f-b277-8f9f2c3eab56")

    # Create tables and insert data
    cu.create_table()
    cu.insert_data()

    # Schedule the update_records method to run every 15 minutes
    schedule.every(2).seconds.do(cu.get_access_token)
    schedule.every(4).seconds.do(cu.update_records)

    # Run the scheduled tasks indefinitely

    while True:
        print("do run_pending")
        schedule.run_pending()
        time.sleep(1)
        
