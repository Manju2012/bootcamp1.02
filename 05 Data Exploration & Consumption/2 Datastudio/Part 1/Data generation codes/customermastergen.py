import csv
from faker import Faker

def fake_data_gen(records):
    fake = Faker('en_IN')
    with open("/home/fagcpdebc02_07/EnC/DSpart1/customer_master.csv",mode='w', newline="") as csvFile:
        writer = csv.DictWriter(csvFile, fieldnames=headers)
        writer.writeheader()
        for i in range(records):
            writer.writerow({
                    'customerid':int(i+1),
                    'name':fake.name(),         
                    'address':fake.street_name(),
                    'city':fake.city(),
                    'state':fake.state(),
                    'pincode':str(fake.postcode())
                    })

if __name__ == '__main__':
    n = int(input('Rows: '))
    headers = ['customerid','name','address','city','state','pincode']
    fake_data_gen(records=n)
    print("CSV generation completed..")-