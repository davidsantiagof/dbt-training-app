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

#from faker.providers.person.en import Provider as PersonProvider
#from faker.providers.color.en_US import Provider as ColorProvider

from faker import Faker
import faker_commerce

fake = Faker()
fake.add_provider(faker_commerce.Provider)

Base = declarative_base()

load_dotenv()

engine_raw = create_engine(
            URL(
              account=os.environ['SNOWFLAKE_ACCOUNT'],
              user=os.environ['SNOWFLAKE_USER'],
              password=os.environ['SNOWFLAKE_PASSWORD'],
              warehouse='newco_wh',
              database='newco_sources',      
              autocommit=True
              )
            )

class Products(Base):
    __tablename__ = "products"
    __table_args__ = {'schema': 'inventory_sys'}

    id              = Column(Integer, Sequence(name="products_id_seq", schema="inventory_sys", start=1000, increment=1), primary_key=True, autoincrement=True)
    name            = Column(String)
    price           = Column(Numeric(38,2))
    type            = Column(String)
    etl_inserted_at = Column(DateTime)
    etl_updated_at  = Column(DateTime)

    def __init__(self, name, price, type, etl_inserted_at, etl_updated_at):
        self.name            = name
        self.price           = price
        self.type            = type
        self.etl_inserted_at = etl_inserted_at
        self.etl_updated_at  = etl_updated_at

class Customers(Base):
    __tablename__ = "customers"
    __table_args__ = {'schema': 'crm_sys'}

    id              = Column(Integer, Sequence(name="customers_id_seq", schema="crm_sys", start=1000, increment=1), primary_key=True, autoincrement=True)
    first_name      = Column(String)
    last_name       = Column(String)
    type            = Column(String)
    etl_inserted_at = Column(DateTime)
    etl_updated_at  = Column(DateTime)

    def __init__(self, first_name, last_name, type, etl_inserted_at, etl_updated_at):
        self.first_name      = first_name
        self.last_name       = last_name
        self.type            = type
        self.etl_inserted_at = etl_inserted_at
        self.etl_updated_at  = etl_updated_at

class Stores(Base):
    __tablename__ = "stores"
    __table_args__ = {'schema': 'assets_sys'}

    id              = Column(Integer, Sequence(name="stores_id_seq", schema="assets_sys", start=1, increment=1), primary_key=True, autoincrement=True)
    name            = Column(String)
    city            = Column(String)
    country         = Column(String)
    etl_inserted_at = Column(DateTime)
    etl_updated_at  = Column(DateTime)

    def __init__(self, name, city, country, etl_inserted_at, etl_updated_at):
        self.name            = name
        self.city            = city
        self.country         = country
        self.etl_inserted_at = etl_inserted_at
        self.etl_updated_at  = etl_updated_at

class Employees(Base):
    __tablename__ = "employees"
    __table_args__ = {'schema': 'human_resources_sys'}

    id              = Column(Integer, Sequence(name="employees_id_seq", schema="human_resources_sys", start=1000, increment=1), primary_key=True, autoincrement=True)
    first_name      = Column(String)
    last_name       = Column(String)
    role            = Column(String)
    store_id        = Column(Integer)
    etl_inserted_at = Column(DateTime)
    etl_updated_at  = Column(DateTime)

    def __init__(self, first_name, last_name, role, store_id, etl_inserted_at, etl_updated_at):
        self.first_name      = first_name
        self.last_name       = last_name
        self.role            = role
        self.store_id        = store_id
        self.etl_inserted_at = etl_inserted_at
        self.etl_updated_at  = etl_updated_at

class Sales(Base):
    
    __tablename__ = "sales"
    __table_args__ = {'schema':'sales_sys'}

    id              = Column(Integer, Sequence(name="sales_id_seq", schema="sales_sys", start=1000, increment=1), primary_key=True, autoincrement=True)
    created_at      = Column(DateTime)
    order_id        = Column(Integer)
    product_id      = Column(Integer)
    quantity        = Column(Integer)
    unit_price      = Column(Numeric(38,2))
    amount          = Column(Numeric(38,2))
    customer_id     = Column(Integer)
    store_id        = Column(Integer)
    employee_id     = Column(Integer)
    etl_inserted_at = Column(DateTime)
    etl_updated_at  = Column(DateTime)
    _is_delayed    = Column(Boolean)
    
    def __init__(self, created_at, order_id, product_id, quantity, unit_price, amount, customer_id, store_id, employee_id, etl_inserted_at, etl_updated_at, _is_delayed= False):
            
        self.created_at      = created_at
        self.order_id        = order_id
        self.product_id      = product_id
        self.quantity        = quantity
        self.unit_price      = unit_price
        self.amount          = amount
        self.customer_id     = customer_id
        self.store_id        = store_id
        self.employee_id     = employee_id
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

def initialize_products(n=10):

    #name_list = [fake.ecommerce_name() for _ in range(n)]
    #price_list = random.choices(range(10,100),k=n)
    #type_list = random.choices(['Clothing','Footwear','Accesories'],k=n)
    #etl_inserted_at_list = [datetime.datetime.now(datetime.UTC)] * n
    #etl_updated_at_list = [datetime.datetime.now(datetime.UTC)] * n
    #
    #output = list(
    #            zip(name_list,price_list,type_list,etl_inserted_at_list,etl_updated_at_list,))
    
    dirname = os.path.dirname(__file__)
    filename = os.path.join(dirname,'./sources/products_init.csv')
    #with open(filename,'w',newline='') as file:
    #    writer = csv.writer(file)
    #    for row in output:
    #        writer.writerow(row)
    
    table_ref = Products.__table_args__['schema']+'.'+Products.__tablename__
    
    base_filename = os.path.basename(filename)

    execute_sql("PUT file://{filename} @~".format(filename=filename))
    execute_sql(
        """
            COPY INTO {table_ref}(name,price,type,etl_inserted_at,etl_updated_at)
            FROM (
                    SELECT f.$1,to_decimal(f.$2,10,2),f.$3,to_timestamp_ntz(f.$4),to_timestamp_ntz(f.$5)
                    FROM @~/{base_filename} f
                )
        """.format(table_ref=table_ref, base_filename = base_filename)
    )
    execute_sql("RM @~ pattern='.*.csv.*'")


def initialize_customers(n=10):

    first_name_list = [fake.first_name() for _ in range(n)]     
    last_name_list =  [fake.last_name() for _ in range(n)]          
    type_list = random.choices(['New','Standard','Premium'],k=n)          
    etl_inserted_at_list = [datetime.datetime.now(datetime.UTC)] * n
    etl_updated_at_list = [datetime.datetime.now(datetime.UTC)] * n

    output = list(
                zip(first_name_list,last_name_list,type_list,etl_inserted_at_list,etl_updated_at_list,))
    
    dirname = os.path.dirname(__file__)
    filename = os.path.join(dirname,'./sources/customers_init.csv')
    #print(output)
    with open(filename,'w',newline='') as file:
        writer = csv.writer(file)
        for row in output:
            writer.writerow(row)
    
    table_ref = Customers.__table_args__['schema']+'.'+Customers.__tablename__
    
    base_filename = os.path.basename(filename)

    execute_sql("PUT file://{filename} @~".format(filename=filename))
    execute_sql(
        """
            COPY INTO {table_ref}(first_name,last_name,type,etl_inserted_at,etl_updated_at)
            FROM (
                    SELECT f.$1,f.$2,f.$3,to_timestamp_ntz(f.$4),to_timestamp_ntz(f.$5)
                    FROM @~/{base_filename} f
                )
        """.format(table_ref=table_ref, base_filename = base_filename)
    )
    execute_sql("RM @~ pattern='.*.csv.*'")

def initialize_stores(n=10):

    dirname = os.path.dirname(__file__)
    filename = os.path.join(dirname,'./sources/stores_init.csv')
    
    table_ref = Stores.__table_args__['schema']+'.'+Stores.__tablename__
    
    base_filename = os.path.basename(filename)

    execute_sql("PUT file://{filename} @~".format(filename=filename))
    execute_sql(
        """
            COPY INTO {table_ref}(name,city,country,etl_inserted_at,etl_updated_at)
            FROM (
                    SELECT f.$1,f.$2,f.$3,to_timestamp_ntz(f.$4),to_timestamp_ntz(f.$5)
                    FROM @~/{base_filename} f
                )
        """.format(table_ref=table_ref, base_filename = base_filename)
    )
    execute_sql("RM @~ pattern='.*.csv.*'")


def initialize_employees(n=30):

    first_name_list = [fake.first_name() for _ in range(n)]     
    last_name_list =  [fake.last_name() for _ in range(n)]          
    role_list = ['Store Manager'] * 10 + ['Staff'] * 20  
    store_id = [num for num in range(1,11)] * 3 
    etl_inserted_at_list = [datetime.datetime.now(datetime.UTC)] * n
    etl_updated_at_list = [datetime.datetime.now(datetime.UTC)] * n

    output = list(
                zip(first_name_list,last_name_list,role_list,store_id,etl_inserted_at_list,etl_updated_at_list,))
    
    dirname = os.path.dirname(__file__)
    filename = os.path.join(dirname,'./sources/employees_init.csv')
    #print(output)
    with open(filename,'w',newline='') as file:
        writer = csv.writer(file)
        for row in output:
            writer.writerow(row)
    
    table_ref = Employees.__table_args__['schema']+'.'+Employees.__tablename__
    
    base_filename = os.path.basename(filename)

    execute_sql("PUT file://{filename} @~".format(filename=filename))
    execute_sql(
        """
            COPY INTO {table_ref}(first_name,last_name,role,store_id,etl_inserted_at,etl_updated_at)
            FROM (
                    SELECT f.$1,f.$2,f.$3,to_number(f.$4),to_timestamp_ntz(f.$5),to_timestamp_ntz(f.$6)
                    FROM @~/{base_filename} f
                )
        """.format(table_ref=table_ref, base_filename = base_filename)
    )
    execute_sql("RM @~ pattern='.*.csv.*'")

def generate_orders(n, start_date_utc_str, end_date_utc_str):
    
    date_format = '%Y-%m-%d %H:%M:%S'
    start_date_utc = datetime.datetime.strptime(start_date_utc_str,date_format)
    end_date_utc = datetime.datetime.strptime(end_date_utc_str,date_format)
    
    sales = []

    Session = sessionmaker(bind=engine_raw)

    with Session() as session:
        products = session.query(Products).all()
        customers = session.query(Customers).all()
        stores = session.query(Stores).all()
        max_order_id = session.query(func.max(Sales.order_id))
        
    td = end_date_utc - start_date_utc
    
    dates_list = sorted(([random.random() * td + start_date_utc for _ in range(n)]), reverse=True)
    
    if max_order_id.scalar() is not None:
        order_id = max_order_id.scalar()+1
    else:
        order_id = 100

    for step in range(n):
        created_at = dates_list.pop()
        customer_id = random.sample(customers,1)[0].id
        store_id = random.sample(stores,1)[0].id
        with Session() as session:
            employees = session.query(Employees).filter(Employees.store_id == store_id).all()
        employee_id = random.sample(employees,1)[0].id
        etl_inserted_at = created_at
        etl_updated_at = created_at
        
        order_products = random.sample(products,random.randint(1,5))
        
        for product in order_products:
            
            product_id = product.id
            quantity = random.randint(1,3)
            unit_price = product.price
            amount = quantity * unit_price

            sales.append([created_at, order_id, product_id, quantity, unit_price, amount, customer_id, store_id, employee_id, etl_inserted_at, etl_updated_at])
        order_id += 1
    dirname = os.path.dirname(__file__)
    filename = os.path.join(dirname,'./sources/sales_init.csv')
    with open(filename,'w',newline='') as file:
        writer = csv.writer(file)
        for row in sales:
            writer.writerow(row)
    
    table_ref = Sales.__table_args__['schema']+'.'+Sales.__tablename__
    
    base_filename = os.path.basename(filename)

    execute_sql("PUT file://{filename} @~".format(filename=filename))
    execute_sql(
        """
            COPY INTO {table_ref}(created_at,order_id,product_id,quantity,unit_price,amount,customer_id,store_id,employee_id,etl_inserted_at,etl_updated_at)
            FROM (
                    SELECT to_timestamp_ntz(f.$1),to_number(f.$2),to_number(f.$3),to_decimal(f.$4,10,2),to_decimal(f.$5,10,2),to_decimal(f.$6,10,2),to_number(f.$7),to_number(f.$8),to_number(f.$9),to_timestamp_ntz(f.$10),to_timestamp_ntz(f.$11)
                    FROM @~/{base_filename} f
                )
        """.format(table_ref=table_ref, base_filename = base_filename)
    )
    execute_sql("RM @~ pattern='.*.csv.*'")

def main():  # 

    while True:
        print("\nOPTIONS")
        print("1: Create Tables")
        print("2: Drop Tables")
        print("3: Initialize Tables")
        print("x: Exit\n")
        option = input("Please enter option: ")
        if option == '1':
            create_table(Products)
            #create_table(Customers)
            #create_table(Stores)
            #create_table(Employees)
            #create_table(Sales)
            print("Tables Created")
        elif option == '2':
            drop_table(Products)
            #drop_table(Customers)
            #drop_table(Stores)
            #drop_table(Employees)
            #drop_table(Sales)
            print("Tables Droped")
        elif option == '3':
            #initialize_products()
            #initialize_customers(10)
            #initialize_stores()
            #initialize_employees()
            generate_orders(800, "2023-01-01 00:00:00", "2023-01-21 23:59:59")
            generate_orders(600, "2023-02-01 00:00:00", "2023-02-28 23:59:59")
            generate_orders(600, "2023-03-01 00:00:00", "2023-03-31 23:59:59")
            generate_orders(500, "2023-04-01 00:00:00", "2023-04-30 23:59:59")
            generate_orders(500, "2023-05-01 00:00:00", "2023-05-31 23:59:59")
            generate_orders(1000, "2023-06-01 00:00:00", "2023-06-30 23:59:59")
            generate_orders(1000, "2023-07-01 00:00:00", "2023-07-31 23:59:59")
            generate_orders(800, "2023-08-01 00:00:00", "2023-08-31 23:59:59")
            generate_orders(700, "2023-09-01 00:00:00", "2023-09-30 23:59:59")
            generate_orders(700, "2023-10-01 00:00:00", "2023-10-31 23:59:59")
            generate_orders(800, "2023-11-01 00:00:00", "2023-11-30 23:59:59")
            generate_orders(2000, "2023-12-01 00:00:00", "2023-12-31 23:59:59")
            print("Tables Initialized")
        elif option == 't':
            generate_orders(5, "2023-01-01 00:00:00", "2023-01-01 23:59:59")
            generate_orders(5, "2023-01-02 00:00:00", "2023-01-02 23:59:59")
        elif option == 'x':
            print("Exiting...")
            break
        else:
            print("Unknown Option\n")

if __name__ == "__main__":
    main()