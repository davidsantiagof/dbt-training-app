import os
import datetime
import random
from typing import Optional

from sqlalchemy import Table, Column, Sequence, create_engine, text, MetaData, Identity

from sqlalchemy.types import SmallInteger, Integer, Numeric, Float, String,  Date, DateTime, Boolean
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func

from snowflake.sqlalchemy import URL, CopyIntoStorage, AWSBucket, CSVFormatter

from dotenv import load_dotenv

import csv

Base = declarative_base()

load_dotenv()

engine_raw = create_engine(
            URL(
              account=os.environ['SNOWFLAKE_ACCOUNT'],
              user=os.environ['SNOWFLAKE_USER'],
              password=os.environ['SNOWFLAKE_PASSWORD'],
              warehouse='newco_wh',
              database='newco_sources_db',      
              autocommit=True
              )
            )

class Sales(Base):
    
    __tablename__ = "sales"
    __table_args__ = {'schema':'sales_sys'}

    id              = Column(Integer, Sequence(name="sale_id_seq", schema="sales_sys", start=1000, increment=1), primary_key=True, autoincrement=True)
    created_at      = Column(DateTime)
    product_id      = Column(Integer)
    quantity        = Column(Integer)
    amount          = Column(Numeric)
    customer_id     = Column(Integer)
    etl_inserted_at = Column(DateTime)
    etl_updated_at  = Column(DateTime)
    
    _is_delayed    = Column(Boolean)
    
    def __init__(self, created_at, product_id, quantity, amount, customer_id, etl_inserted_at, etl_updated_at, _is_delayed= False):
            
        self.created_at      = created_at
        self.product_id      = product_id
        self.quantity        = quantity
        self.amount          = amount
        self.customer_id     = customer_id
        self.etl_inserted_at = etl_inserted_at
        self.etl_updated_at  = etl_updated_at
        
        self._is_delayed     = _is_delayed
        
def create_table(Table):
    Base.metadata.tables[Table.__table_args__['schema']+'.'+Table.__tablename__].create(engine_raw)

def drop_table(Table):
    Base.metadata.tables[Table.__table_args__['schema']+'.'+Table.__tablename__].drop(engine_raw)

def execute_sql(sql):
    Session = sessionmaker(bind=engine_raw)
    with Session() as session:
        result = session.execute(sql)
        session.commit()
    return result

def generate_csv(filename, StartDateTime, EndDateTime, NumSales):
    
    td = EndDateTime - StartDateTime
    
    created_at_list      = sorted(([StartDateTime + random.random() * td for _ in range(NumSales)]))
    product_id_list      = random.choices(range(100,102),k=NumSales)                                    #Two Products Company (product_id = 100, 101) (for now)
    quantity_list        = random.choices(range(1,6),k=NumSales)
    amount_list          = [q * 100 for q in quantity_list]                                             #Fixed Unit Price (Cents) (100)
    customer_id_list     = random.choices(range(100,199),k=NumSales)                                    #Fixed Number of Customers
    etl_inserted_at_list = created_at_list
    etl_updated_at_list  = created_at_list
    _is_delayed_list     = [False] * NumSales


    output = list(
                zip(created_at_list,product_id_list,quantity_list,amount_list,customer_id_list,etl_inserted_at_list,etl_updated_at_list,_is_delayed_list))
    
    #dirname = os.path.dirname(__file__)
    #filename = os.path.join(dirname,'./sources/init_load.csv')
    #print(output)
    with open(filename,'w',newline='') as file:
        writer = csv.writer(file)
        for row in output:
            writer.writerow(row)
    
def load_csv(filename, Table):
    
    table_ref = Table.__table_args__['schema']+'.'+Table.__tablename__
    base_filename = os.path.basename(filename)

    execute_sql("PUT file://{filename} @~".format(filename=filename))
    execute_sql(
        """
            COPY INTO {table_ref}(created_at,product_id,quantity,amount,customer_id,etl_inserted_at,etl_updated_at,_is_delayed)
            FROM (
                    SELECT to_timestamp_ntz(f.$1),to_number(f.$2),to_number(f.$3),to_number(f.$4),to_number(f.$5),to_timestamp_ntz(f.$6),to_timestamp_ntz(f.$7),to_boolean(f.$8)
                    FROM @~/{base_filename} f
                )
        """.format(table_ref=table_ref, base_filename = base_filename)
    )
    execute_sql("RM @~ pattern='.*.csv.*'")

    
def insert_sale(delayed_flag=0):
    
    date_format = '%Y-%m-%d %H:%M:%S'
    
    Session = sessionmaker(bind=engine_raw)

    with Session() as session:
        result_max_created_at = session.query(func.max(Sales.created_at)).first()                       #list
        result_max_etl_inserted_at = session.query(func.max(Sales.etl_inserted_at)).first()             #list

    max_created_at = result_max_created_at[0]
    max_etl_inserted_at = result_max_etl_inserted_at[0]
    
    if delayed_flag == 0:
        created_at = max_etl_inserted_at + datetime.timedelta(seconds=random.randint(1,10))
        etl_inserted_at = created_at
        etl_updated_at = created_at
        is_delayed = False
        print('max_created_at: ' + max_created_at.strftime(date_format))
        print('created_at: ' + created_at.strftime(date_format))
        print('etl_inserted_at: ' + etl_inserted_at.strftime(date_format))
        print('etl_updated_at: ' + etl_updated_at.strftime(date_format))
    elif delayed_flag == 1:
        print("Last Created Sale At: " + max_created_at.strftime(date_format))
        print("Last Inserted Sale At: " + max_etl_inserted_at.strftime(date_format))
        delay_sec = input("Insert Sale => Type Delay (Seconds): ")
        delay_sec_fmt = int(delay_sec)
        created_at = max_created_at - datetime.timedelta(seconds=delay_sec_fmt)
        etl_inserted_at = max_etl_inserted_at + datetime.timedelta(seconds=random.randint(1,10))
        etl_updated_at = etl_inserted_at
        is_delayed = True
        print('created_at (delayed): ' + created_at.strftime(date_format))
        print('etl_inserted_at (delayed): ' + etl_inserted_at.strftime(date_format))
        print('etl_updated_at (delayed): ' + etl_updated_at.strftime(date_format))
    
    product_id = random.randint(100,101)                                    #Two Products Company (product_id = 100, 101) (for now)
    quantity = random.randint(1,5)
    amount = quantity * 100
    customer_id = random.randint(100,199)
    
    with Session() as session:
        sale = Sales(   
                        created_at      = created_at,
                        product_id      = product_id,
                        quantity        = quantity,
                        amount          = amount,
                        customer_id     = customer_id,
                        etl_inserted_at = etl_inserted_at,
                        etl_updated_at  = etl_updated_at,
                        _is_delayed     = is_delayed
                    ) 
        session.add(sale)
        session.commit()
        session.refresh(sale)
    return sale

def main():  # 

    while True:
        print("\nOPTIONS")
        print("c: Create sales Table")
        print("d: Drop sales Table")
        print("1: Load Sales")
        print("2: Insert Single Sale")
        print("3: Insert Single Delayed Sale")
        print("x: Exit\n")
        option = input("Please enter option: ")
        if option == 'c':
            create_table(Sales)
            print("Table Created OK")
        elif option == 'd':
            drop_table(Sales)
            print("Table Droped OK")
        elif option == '1':
            date_format = '%Y-%m-%d %H:%M:%S'

            start_date = input("Load Sales => Type StartDate (YYYY-MM-DD HH:MM:SS): ")
            start_date_fmt = datetime.datetime.strptime(start_date,date_format)

            end_date = input("Load Sales => Type EndDate (YYYY-MM-DD HH:MM:SS): ")
            end_date_fmt = datetime.datetime.strptime(end_date,date_format)

            num_sales = input("Load Sales => Type NumSales: ")
            num_sales_fmt = int(num_sales)
            
            dirname = os.path.dirname(__file__)
            filename = os.path.join(dirname,'./sources/init_load.csv')
            generate_csv(filename,start_date_fmt, end_date_fmt, num_sales_fmt)
            load_csv(filename,Sales)
            print("CSV Loaded OK") 
        elif option == '2':
            print("Inserting Single Sale:")
            insert_sale(0)
            print("Single Sale Inserted OK")
        elif option == '3':
            print("Inserting Single Delayed Sale:")
            insert_sale(1)
            print("Delayed Single Sale Inserted OK")
        elif option == 'x':
            print("Exiting...")
            break
        else:
            print("Unknown Option\n")

if __name__ == "__main__":
    main()