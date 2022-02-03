import csv
from faker import Faker
import datetime


def fake_data_gen(records, headers, mode):
    fake = Faker('en_US')
    fake1 = Faker('en_GB')   # To generate phone numbers

    with open("SnD/workingfile/sourcefile1.csv", mode=mode, newline="") as csvFile:
        writer = csv.DictWriter(csvFile, fieldnames=headers)
        #writer.writeheader()  #commented for modifying sourcefile1.csv in workingfile

        for _ in range(records):
            writer.writerow({
                    "Name": fake.first_name(),
                    "Birth Date" : fake.date(pattern="%Y-%m-%d", end_datetime=datetime.date(2021,1,1)),
                    "Phone Number" : fake1.phone_number(),
                    "Citizen" : fake.random.choice([True, False]),
                    "Country" : fake.country(),
                    "Zip Code" : fake.zipcode(),
                    "Year" : fake.year(),
                    "Time": fake.time(),
                    "Longitude": fake.longitude(),
                    "Latitude" : fake.latitude() 
                    })

if __name__ == '__main__':

    while True:
        try:
            MODE = int(input('Choose mode- 1:write, 2:append:')) 
            RECORDS = int(input('Enter how many rows to be inserted:')) # To Enter required number of data
            break

        except ValueError:
            print('Wrong input. Please Enter correct number')

        except NameError:
            print('Wrong input. Please Enter correct number')


    headers = ["Name", "Birth Date", "Phone Number","Citizen", "Country",
                "Zip Code","Year", "Time","Longitude","Latitude"]

    mode = 'wt' if MODE == 1 else 'a'  #wt:write mode,a:append mode

    fake_data_gen(records=RECORDS, headers=headers, mode=mode)

    print("CSV generation completed...")