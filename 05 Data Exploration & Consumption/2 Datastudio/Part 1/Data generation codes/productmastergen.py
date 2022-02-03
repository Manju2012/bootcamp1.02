import csv
from faker import Faker

def fake_data_gen(records):
    fake = Faker('en_IN')
    with open("/home/fagcpdebc02_07/EnC/DSpart1/product_master.csv",mode='w', newline="") as csvFile:
        writer = csv.DictWriter(csvFile, fieldnames=headers)
        writer.writeheader()
        for i in range(records):
            writer.writerow({
                    'productid': i+1,
                    'productcode': f'A{i+1}',
                    'productname': fake.random.choice([ 'Sugar', 'Grains', 'Salt','Tea','Milk', 'OtherItems', 'Flour', 'Oil']),
                    'sku': str(fake.random.randint(1,5))+'KG',
                    'rate':fake.random.randint(100,1000),
                    'isactive':fake.random.choice(['True', 'False'])
                    })

if __name__ == '__main__':
    n = int(input('Rows: '))
    headers = ['productid', 'productcode',  'productname',  'sku',  'rate','isactive' ]
    fake_data_gen(records=n)
    print("CSV generation completed..")