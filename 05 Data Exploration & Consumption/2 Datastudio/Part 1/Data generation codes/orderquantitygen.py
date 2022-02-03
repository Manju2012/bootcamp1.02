import csv
from faker import Faker

def fake_data_gen(records):
    fake = Faker('en_IN')
    with open("/home/fagcpdebc02_07/EnC/DSpart1/order_quantity.csv",mode='w', newline="") as csvFile:
        writer = csv.DictWriter(csvFile, fieldnames=headers)
        writer.writeheader()
        for i in range(records):
            writer.writerow({
                    'orderid': int(i+1),
                    'productid': fake.random.randint(1,50),
                    'quantity' : fake.random.randint(1,20)
})
if __name__ == '__main__':
    n = int(input('Rows: '))
    headers = ['orderid',   'productid',    'quantity']
    fake_data_gen(records=n)
    print("CSV generation completed..")