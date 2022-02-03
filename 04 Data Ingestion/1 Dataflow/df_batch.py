import apache_beam as beam
import argparse 
from apache_beam.options.pipeline_options import PipelineOptions 
from sys import argv

PROJECT_ID= 'ssbc-99'
SCHEMA = 'Name1:STRING,Birth_Date1:STRING,Phone_Number1:STRING,Citizen1:STRING,Country1:STRING,Zip_Code1:STRING,Year1:STRING,Time1:STRING,Longitude1:STRING,Latitude1:STRING,Name2:STRING,Birth_Date2:STRING,Phone_Number2:STRING,Citizen2:STRING,Country2:STRING,Zip_Code2:STRING,Year2:STRING,Time2:STRING,Longitude2:STRING,Latitude2:STRING,Name3:STRING,Birth_Date3:STRING,Phone_Number3:STRING,Citizen3:STRING,Country3:STRING,Zip_Code3:STRING,Year3:STRING,Time3:STRING,Longitude3:STRING,Latitude3:STRING,Name4:STRING,Birth_Date4:STRING,Phone_Number4:STRING,Citizen4:STRING,Country4:STRING,Zip_Code4:STRING,Year4:STRING,Time4:STRING,Longitude4:STRING,Latitude4:STRING,Name5:STRING,Birth_Date5:STRING,Phone_Number5:STRING,Citizen5:STRING,Country5:STRING,Zip_Code5:STRING,Year5:STRING,Time5:STRING,Longitude5:STRING,Latitude5:STRING'

if __name__ =='__main__':
    parser= argparse.ArgumentParser()
    known_args= parser.parse_known_args(argv)
    p= beam.Pipeline(options=PipelineOptions())
    (p| 'ReadData' >>beam.io.ReadFromText('gs://msn-dfpart/f100k.csv', skip_header_lines =1)
        |'SplitData' >> beam.Map(lambda x: x.split(','))
        |'FormatToDict' >> beam.Map(lambda x: {            
            "Name1":x[0],
            "Birth_Date1":x[1],
            "Phone_Number1":x[2],
            "Citizen1":x[3],
            "Country1":x[4],
            "Zip_Code1":x[5],
            "Year1":x[6],
            "Time1":x[7],
            "Longitude1":x[8],
            "Latitude1":x[9],
            "Name2":x[10],
            "Birth_Date2":x[11],
            "Phone_Number2":x[12],
            "Citizen2":x[13],
            "Country2":x[14],
            "Zip_Code2":x[15],
            "Year2":x[16],
            "Time2":x[17],
            "Longitude2":x[18],
            "Latitude2":x[19],
            "Name3":x[20],
            "Birth_Date3":x[21],
            "Phone_Number3":x[22],
            "Citizen3":x[23],
            "Country3":x[24],
            "Zip_Code3":x[25],
            "Year3":x[26],
            "Time3":x[27],
            "Longitude3":x[28],
            "Latitude3":x[29],
            "Name4":x[30],
            "Birth_Date4":x[31],
            "Phone_Number4":x[32],
            "Citizen4":x[33],
            "Country4":x[34],
            "Zip_Code4":x[35],
            "Year4":x[36],
            "Time4":x[37],
            "Longitude4":x[38],
            "Latitude4":x[39],
            "Name5":x[40],
            "Birth_Date5":x[41],
            "Phone_Number5":x[42],
            "Citizen5":x[43],
            "Country5":x[44],
            "Zip_Code5":x[45],
            "Year5":x[46],
            "Time5":x[47],
            "Longitude5":x[48],
            "Latitude5":x[49]
        })
        |'WriteToBigQuery' >> beam.io.WriteToBigQuery('{0}:dfbatch.t100k'.format(PROJECT_ID),
        schema=SCHEMA,method="STREAMING_INSERTS",write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND))
    result = p.run()