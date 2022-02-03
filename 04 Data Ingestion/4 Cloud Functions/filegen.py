import csv
from faker import Faker

def fake_data_gen(records, headers, mode):
    fake = Faker('en_US')
    with open("/home/fagcpdebc02_07/Ingestion/function/cf_file5.csv", mode=mode, newline="") as csvFile:
        writer = csv.DictWriter(csvFile, fieldnames=headers)
        writer.writeheader()

        for _ in range(records):
            writer.writerow({
                    "Name": fake.name(), 
                    "Fav_number": fake.random.randint(1,50),
                    "Fav_colour": fake.random.choice(['pink','purple','blue','yellow','red','white','black','lime','peach']) 
                    })

if __name__ == '__main__':
    while True:
        try:
            RECORDS = int(input('Enter no. of records to be inserted:')) 
            break
        except ValueError:
            print('Wrong input. Please Enter correct number')
        except NameError:
            print('Wrong input. Please Enter correct number')

    headers = ["Name", "Fav_number","Fav_colour"]
    mode = 'wt' 
    fake_data_gen(records=RECORDS, headers=headers, mode=mode)
    print("CSV generation completed...")
