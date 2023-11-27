import os
import datetime
import random
from typing import Optional

from sqlalchemy import Table, Column, Sequence, create_engine, text, MetaData, Identity

from sqlalchemy.types import Integer, Numeric, Float, String,  Date, DateTime, Boolean
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func

from snowflake.sqlalchemy import URL, CopyIntoStorage, AWSBucket, CSVFormatter

from dotenv import load_dotenv

import wget
import csv

from faker import Faker

import pandas as pd

##import faker_commerce

fake = Faker()
##fake.add_provider(faker_commerce.Provider)

Base = declarative_base()

load_dotenv()

engine_raw = create_engine(
            URL(
              account=os.environ['SNOWFLAKE_ACCOUNT'],
              user=os.environ['SNOWFLAKE_USER'],
              #authenticator='externalbrowser',
              password=os.environ['SNOWFLAKE_PASSWORD'],
              warehouse='transforming',
              database='raw_sources',      
              #schema ='public',
              autocommit=True
              )
              #,echo=True
        )



class Sales(Base):
    
    __tablename__ = "sales"
    __table_args__ = {'schema':'jaffle_shop'}

    created_at      = Column(DateTime)
    product_id      = Column(Integer)
    quantity        = Column(Integer)
    amount          = Column(Numeric)
    customer_id     = Column(Integer)
    invoice_id      = Column(Integer)
    etl_inserted_at = Column(DateTime)
    etl_updated_at  = Column(DateTime)
    
    id              = Column(Integer, Sequence(name="sales_id_seq", schema="jaffle_shop", start=1000, increment=1), primary_key=True, autoincrement=True)
    
    def __init__(self, created_at, product_id, quantity, amount, customer_id, invoice_id, etl_inserted_at, etl_updated_at, id=None):
            
        self.created_at      = created_at
        self.product_id      = product_id
        self.quantity        = quantity
        self.amount          = amount
        self.customer_id     = customer_id
        self.invoice_id      = invoice_id
        self.etl_inserted_at = etl_inserted_at
        self.etl_updated_at  = etl_updated_at

        self.id              = id
        
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

def load_sales(StartDateTime, EndDateTime, NumSales):
    a = 1

    td = EndDateTime - StartDateTime
    created_at_array = sorted(([StartDateTime + random.random() * td for _ in range(NumSales)]))

    product_id_array = random.choices(range(100,110),k=NumSales)
    quantity_array = random.choices(range(1,5),k=NumSales)
    amount_array = random.choices(range(100,1000),k=NumSales)
    customer_id_array = random.choices(range(100,199),k=NumSales)
    invoice_id_array = [0] * NumSales
    etl_inserted_at_list = created_at_array
    etl_updated_at_list =  created_at_array

    Session = sessionmaker(bind=engine_raw)

    with Session() as session:
        for row in range(NumSales):
            
            sale = Sales(   created_at=created_at_array[row],
                            product_id=product_id_array[row],
                            quantity=quantity_array[row],
                            amount=amount_array[row],
                            customer_id=customer_id_array[row],
                            invoice_id=invoice_id_array[row],
                            etl_inserted_at=etl_inserted_at_list[row],
                            etl_updated_at=etl_updated_at_list[row]
                        ) 
            session.add(sale)
            session.commit()
            session.refresh(sale)

def generate_csv(filename, StartDateTime, EndDateTime, NumSales):
    
    td = EndDateTime - StartDateTime
    created_at_array = sorted(([StartDateTime + random.random() * td for _ in range(NumSales)]))

    product_id_array = random.choices(range(100,110),k=NumSales)
    quantity_array = random.choices(range(1,5),k=NumSales)
    amount_array = random.choices(range(100,1000),k=NumSales)
    customer_id_array = random.choices(range(100,199),k=NumSales)
    invoice_id_array = [0] * NumSales
    etl_inserted_at_list = created_at_array
    etl_updated_at_list =  created_at_array

    output = list(
                zip(created_at_array,product_id_array,quantity_array,amount_array,customer_id_array,invoice_id_array,etl_inserted_at_list,etl_updated_at_list))
    
    #dirname = os.path.dirname(__file__)
    #filename = os.path.join(dirname,'./sources/init_load.csv')

    with open(filename,'w',newline='') as file:
        writer = csv.writer(file)

        for row in output:
            writer.writerow(row)
    
def load_csv(filename):
    execute_sql("PUT file://{} @~".format(filename))
    execute_sql(
        """
            COPY INTO jaffle_shop.sales(created_at,product_id,quantity,amount,customer_id,invoice_id,etl_inserted_at,etl_updated_at)
            FROM (
                    SELECT to_timestamp_ntz(f.$1),to_number(f.$2),to_number(f.$3),to_number(f.$4),to_number(f.$5),to_number(f.$6),to_timestamp_ntz(f.$7),to_timestamp_ntz(f.$8)
                    FROM @~/{} f
                )
        """.format(os.path.basename(filename))
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
        print('max_created_at: ' + max_created_at.strftime(date_format))
        print('created_at: ' + created_at.strftime(date_format))
        print('etl_inserted_at: ' + etl_inserted_at.strftime(date_format))
        print('etl_updated_at: ' + etl_updated_at.strftime(date_format))
    elif delayed_flag == 1:
        print("Current MAX(Sales.created_at): " + max_created_at.strftime(date_format))
        delay_sec = input("Insert Sale => Type Delay (Seconds): ")
        delay_sec_fmt = int(delay_sec)
        created_at = max_created_at - datetime.timedelta(seconds=delay_sec_fmt)
        etl_inserted_at = max_etl_inserted_at + datetime.timedelta(seconds=random.randint(1,10))
        etl_updated_at = etl_inserted_at
        print('created_at (delayed): ' + created_at.strftime(date_format))
        print('etl_inserted_at (delayed): ' + etl_inserted_at.strftime(date_format))
        print('etl_updated_at (delayed): ' + etl_updated_at.strftime(date_format))
    
    product_id = random.randint(100,110)
    quantity = random.randint(1,5)
    amount = random.randint(100,1000)
    customer_id = random.randint(100,199)
    invoice_id = 0
    
    with Session() as session:
        sale = Sales(   
                        created_at      = created_at,
                        product_id      = product_id,
                        quantity        = quantity,
                        amount          = amount,
                        customer_id     = customer_id,
                        invoice_id      = invoice_id,
                        etl_inserted_at = etl_inserted_at,
                        etl_updated_at  = etl_updated_at
                    ) 
        session.add(sale)
        session.commit()
        session.refresh(sale)
    return sale



def Xmain():
    delay_flag = input("Insert Sale => Delay? 01")
    inserted = insert_sale(int(delay_flag))
    print(inserted)

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
            #print(start_date_fmt)
            #print(type(start_date_fmt))

            end_date = input("Load Sales => Type EndDate (YYYY-MM-DD HH:MM:SS): ")
            end_date_fmt = datetime.datetime.strptime(end_date,date_format)
            #print(end_date_fmt)
            #print(type(end_date_fmt))

            num_sales = input("Load Sales => Type NumSales: ")
            num_sales_fmt = int(num_sales)
            #print(num_sales_fmt)
            #print(type(num_sales_fmt))
            
            dirname = os.path.dirname(__file__)
            filename = os.path.join(dirname,'./sources/init_load.csv')
            generate_csv(filename,start_date_fmt, end_date_fmt, num_sales_fmt)
            load_csv(filename)
            print("CSV Loaded?") 
        elif option == '2':
            print("Inserting Single Sale:")
            insert_sale(0)
        elif option == '3':
            print("Inserting Single Delayed Sale:")
            insert_sale(1)
        elif option == 'x':
            print("Exiting...")
            break
        else:
            print("Unknown Option\n")

if __name__ == "__main__":
    main()

###############################################
###############################################
###############################################

class IncrementalEvents(Base):
    __tablename__ = "incremental_events"
    __table_args__ = {'schema': 'jaffle_shop'}
    
    event_id = Column(Integer, Sequence(name="event_id_seq", schema="jaffle_shop", start=1000, increment=1), primary_key=True, autoincrement=True)
    resource_id = Column(Integer)
    event_type = Column(String)
    event_value = Column(Integer)
    event_created_at = Column(DateTime)
    etl_inserted_at = Column(DateTime,  default=datetime.datetime.utcnow)
    etl_updated_at = Column(DateTime,  default=datetime.datetime.utcnow)

    def __init__(self, resource_id, event_type, event_value, event_created_at, event_id=None, etl_inserted_at=None, etl_updated_at=None):
        self.event_id = event_id
        self.resource_id = resource_id
        self.event_type = event_type
        self.event_value = event_value
        self.event_created_at = event_created_at
        self.etl_inserted_at = etl_inserted_at
        self.etl_updated_at = etl_updated_at
    
class SnapshotProducts(Base):
    __tablename__ = "snapshot_products"
    __table_args__ = {'schema': 'jaffle_shop'}
    
    product_id = Column(Integer, Sequence(name="product_id_seq", schema="jaffle_shop", start=1000, increment=1), primary_key=True, autoincrement=True)
    product_name = Column(String)
    product_price = Column(Integer)
    product_created_at = Column(DateTime)
    product_updated_at = Column(DateTime)
    etl_inserted_at = Column(DateTime,  default=datetime.datetime.utcnow)
    etl_updated_at = Column(DateTime,  default=datetime.datetime.utcnow)

    def __init__(self, product_name, product_price, product_created_at, product_updated_at, product_id=None, etl_inserted_at=None, etl_updated_at=None):
        self.product_id = product_id
        self.product_name = product_name
        self.product_price = product_price
        self.product_created_at = product_created_at
        self.product_updated_at = product_updated_at
        self.etl_inserted_at = etl_inserted_at
        self.etl_updated_at = etl_updated_at

    def __repr__(self):
        return f"[PRODUCT => product_id: {self.product_id}, product_name: {self.product_name}, product_price: {self.product_price}, product_created_at: {self.product_created_at}, product_updated_at: {self.product_updated_at}, etl_inserted_at: {self.etl_inserted_at}, etl_updated_at: {self.etl_updated_at}]\n"



def create_incremental_events_table():
    #Base.metadata.create_all(engine_raw)
    Base.metadata.tables['jaffle_shop.incremental_events'].create(engine_raw)

def create_snapshot_products_table():
    #Base.metadata.create_all(engine_raw)
    Base.metadata.tables['jaffle_shop.snapshot_products'].create(engine_raw)

def get_incremtal_events():
    Session = sessionmaker(bind=engine_raw)

    with Session() as session:
        events = session.query(IncrementalEvents)

    return events.all()

def create_event_on_new_resource(delayed_flag=0):
    Session = sessionmaker(bind=engine_raw)

    with Session() as session:

        current_resource_id = session.query(func.max(IncrementalEvents.resource_id))

        if current_resource_id.scalar() is not None:
            current_resource_id = current_resource_id.scalar() 
        else:
            current_resource_id = 0

        event_types = ['A','B','C','D']

        random_event_type_idx = random.randint(1,4)-1

        temp_created_at = datetime.datetime.utcnow() 

        if delayed_flag == 0:
            event_created_at = temp_created_at 
        elif delayed_flag == 1:
            event_created_at = temp_created_at - datetime.timedelta(seconds=random.randint(30,180))

        event = IncrementalEvents(
                            resource_id = current_resource_id + 1,
                            event_type = event_types[random_event_type_idx],
                            event_value = random.randint(0,1000),
                            event_created_at = event_created_at
                            ) 
        session.add(event)
        session.commit()
        session.refresh(event)
    
    return event

def create_event_on_existing_resource(delayed_flag=0):
    Session = sessionmaker(bind=engine_raw)

    with Session() as session:

        resources = session.query(IncrementalEvents.resource_id).distinct().all()
        #print(resources)
        sample_resource = random.sample(resources,1)
        
        event_types = ['A','B','C','D']

        random_event_type_idx = random.randint(1,4)-1

        temp_created_at = datetime.datetime.utcnow() 

        if delayed_flag == 0:
            event_created_at = temp_created_at 
        elif delayed_flag == 1:
            event_created_at = temp_created_at - datetime.timedelta(seconds=random.randint(30,180))

        event = IncrementalEvents(
                            resource_id = sample_resource[0].resource_id,
                            event_type = event_types[random_event_type_idx],
                            event_value = random.randint(0,1000),
                            event_created_at = event_created_at
                            ) 
        session.add(event)
        session.commit()
        session.refresh(event)
    
    return event


def create_new_product():
    Session = sessionmaker(bind=engine_raw)

    with Session() as session:

       

        product_name = fake.ecommerce_name()
        product_price = random.randint(10,1000)

        product = SnapshotProducts(
                            product_name = product_name,
                            product_price = product_price,
                            product_created_at = datetime.datetime.utcnow(),
                            product_updated_at = datetime.datetime.utcnow()
                            ) 
        session.add(product)
        session.commit()
        session.refresh(product)
    
    return product

def update_existing_product_price():
    Session = sessionmaker(bind=engine_raw)
    with Session() as session:

        products = session.query(SnapshotProducts).all()
        sample_product = random.sample(products,1)

        new_product_price = random.randint(10,1000)

        updated_product = session.get(SnapshotProducts,sample_product[0].product_id)

        updated_product.product_price = new_product_price
        updated_product.product_updated_at = datetime.datetime.utcnow() 
        updated_product.etl_updated_at = datetime.datetime.utcnow()

        session.add(updated_product)
        session.commit()
        session.refresh(updated_product)
    
    return updated_product





    




