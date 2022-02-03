import csv
from faker import Faker
import datetime

def fake_data_gen(records):
    fake = Faker('en_IN')
    with open("/home/fagcpdebc02_07/EnC/DSpart1/order_details.csv",mode='w', newline="") as csvFile:
        writer = csv.DictWriter(csvFile, fieldnames=headers)
        writer.writeheader()

        for i in range(records):
            dt = fake.date_time()
            writer.writerow({
                    'orderid': i+1 ,
                    'customerid':fake.random.randint(1,10),
                    'orderplaceddatetime':dt,
                    'ordercompletiondatetime':dt,
                    'orderstatus': fake.random.choice(['InProgress','Completed','Cancelled'])
                    })

if __name__ == '__main__':
    n = int(input('Rows: '))
    headers = ['orderid',   'customerid',   'orderplaceddatetime',  'ordercompletiondatetime',  'orderstatus']
    fake_data_gen(records=n)
    print("CSV generation completed..")