import csv
from faker import Faker
import datetime


def fake_data_gen(records, headers, mode):
    fake = Faker('en_US')
    fake1 = Faker('en_GB')   # To generate phone numbers

    with open("/home/fagcpdebc02_07/Ingestion/f5.csv", mode=mode, newline="") as csvFile:
        writer = csv.DictWriter(csvFile, fieldnames=headers)
        writer.writeheader()  

        for _ in range(records):
            writer.writerow({
                    "Name1": fake.first_name(),
                    "Birth_Date1" : fake.date(pattern="%Y-%m-%d", end_datetime=datetime.date(2021,1,1)),
                    "Phone_Number1" : fake1.phone_number(),
                    "Citizen1" : fake.random.choice([True, False]),
                    "Country1" : fake.country(),
                    "Zip_Code1" : fake.zipcode(),
                    "Year1" : fake.year(),
                    "Time1": fake.time(),
                    "Longitude1": fake.longitude(),
                    "Latitude1" : fake.latitude(), 
                    "Name2" : fake.first_name(),
                    "Birth_Date2" : fake.date(pattern="%Y-%m-%d", end_datetime=datetime.date(2021,1,1)),
                    "Phone_Number2" : fake1.phone_number(),
                    "Citizen2" : fake.random.choice([True, False]),
                    "Country2" : fake.country(),
                    "Zip_Code2" : fake.zipcode(),
                    "Year2" : fake.year(),
                    "Time2": fake.time(),
                    "Longitude2": fake.longitude(),
                    "Latitude2" : fake.latitude() ,
                    "Name3": fake.first_name(),
                    "Birth_Date3" : fake.date(pattern="%Y-%m-%d", end_datetime=datetime.date(2021,1,1)),
                    "Phone_Number3" : fake1.phone_number(),
                    "Citizen3" : fake.random.choice([True, False]),
                    "Country3" : fake.country(),
                    "Zip_Code3" : fake.zipcode(),
                    "Year3" : fake.year(),
                    "Time3": fake.time(),
                    "Longitude3": fake.longitude(),
                    "Latitude3" : fake.latitude() ,
                    "Name4": fake.first_name(),
                    "Birth_Date4" : fake.date(pattern="%Y-%m-%d", end_datetime=datetime.date(2021,1,1)),
                    "Phone_Number4" : fake1.phone_number(),
                    "Citizen4" : fake.random.choice([True, False]),
                    "Country4" : fake.country(),
                    "Zip_Code4" : fake.zipcode(),
                    "Year4" : fake.year(),
                    "Time4": fake.time(),
                    "Longitude4": fake.longitude(),
                    "Latitude4" : fake.latitude() ,
                    "Name5": fake.first_name(),
                    "Birth_Date5" : fake.date(pattern="%Y-%m-%d", end_datetime=datetime.date(2021,1,1)),
                    "Phone_Number5" : fake1.phone_number(),
                    "Citizen5" : fake.random.choice([True, False]),
                    "Country5" : fake.country(),
                    "Zip_Code5" : fake.zipcode(),
                    "Year5" : fake.year(),
                    "Time5": fake.time(),
                    "Longitude5": fake.longitude(),
                    "Latitude5" : fake.latitude() 
                    })

if __name__ == '__main__':

    while True:
        try: 
            RECORDS = int(input('Enter how many rows to be inserted:')) # To Enter required number of data
            break

        except ValueError:
            print('Wrong input. Please Enter correct number')

        except NameError:
            print('Wrong input. Please Enter correct number')


    headers = ["Name1", "Birth_Date1", "Phone_Number1","Citizen1", "Country1",
                "Zip_Code1","Year1", "Time1","Longitude1","Latitude1",
                "Name2", "Birth_Date2", "Phone_Number2","Citizen2", "Country2",
                "Zip_Code2","Year2", "Time2","Longitude2","Latitude2",
                "Name3", "Birth_Date3", "Phone_Number3","Citizen3", "Country3",
                "Zip_Code3","Year3", "Time3","Longitude3","Latitude3",
                "Name4", "Birth_Date4", "Phone_Number4","Citizen4", "Country4",
                "Zip_Code4","Year4", "Time4","Longitude4","Latitude4",
                "Name5", "Birth_Date5", "Phone_Number5","Citizen5", "Country5",
                "Zip_Code5","Year5", "Time5","Longitude5","Latitude5"
                ]

    mode = 'wt' 

    fake_data_gen(records=RECORDS, headers=headers, mode=mode)

    print("CSV generation completed...")