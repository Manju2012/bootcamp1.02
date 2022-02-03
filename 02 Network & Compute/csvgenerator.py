import csv
from faker import Faker
import datetime

def fake_data_gen(records, headers, mode):
    fake = Faker('en_US')
    fake1 = Faker('en_GB')   # To generate phone numbers

    with open("vmreccp.csv", mode=mode, newline="") as csvFile:
        writer = csv.DictWriter(csvFile, fieldnames=headers)
        writer.writeheader()

        for _ in range(records):
            writer.writerow({
                    "Name": fake.first_name(),
                    "Birth Date" : fake.date(pattern="%Y-%m-%d", end_datetime=datetime.date(2021, 1,1)),
                    "Phone Number" : fake1.phone_number(),
                    "Citizen" : fake.random.choice([True, False]),
                    "Country" : fake.country(),
                    "Zip Code" : fake.zipcode(),
                    "Year" : fake.year(),
                    "Time": fake.time(),
                    "Longitude": fake.longitude(),
                    "Latitude" : fake.latitude(), 
                    "Father_Name": fake.first_name(),
                    "Birth Date" : fake.date(pattern="%Y-%m-%d", end_datetime=datetime.date(2021, 1,1)),
                    "Phone Number" : fake1.phone_number(),
                    "Citizen" : fake.random.choice([True, False]),
                    "Country" : fake.country(),
                    "Zip Code" : fake.zipcode(),
                    "Year" : fake.year(),
                    "Time": fake.time(),
                    "Longitude": fake.longitude(),
                    "Latitude" : fake.latitude(),
                    "Mother_Name": fake.first_name(),
                    "Birth Date" : fake.date(pattern="%Y-%m-%d", end_datetime=datetime.date(2021, 1,1)),
                    "Phone Number" : fake1.phone_number(),
                    "Citizen" : fake.random.choice([True, False]),
                    "Country" : fake.country()
                    })

if __name__ == '__main__':

    while True:
        try:
            RECORDS = int(input('Enter how many rows to be inserted:')) 
            break

        except ValueError:
            print('Wrong input. Please Enter correct number')

        except NameError:
            print('Wrong input. Please Enter correct number')


    headers = ["Name", "Birth Date", "Phone Number","Citizen", "Country","Zip Code","Year", "Time","Longitude","Latitude",
                "Father_Name", "Birth Date", "Phone Number","Citizen", "Country","Zip Code","Year", "Time","Longitude","Latitude",
                "Mother_Name", "Birth Date", "Phone Number","Citizen", "Country"]

    mode = 'wt' 

    fake_data_gen(records=RECORDS, headers=headers, mode=mode)

    print("CSV generation completed...")
