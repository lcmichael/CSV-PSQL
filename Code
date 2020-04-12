# 将CSV导入SQL
需要时自提

#首先要在PostgreSQL里创建table，输入列名格式条件等
#再在jupyter输入以下
import csv
import pprint
data_super= list(csv.DictReader(open('Sample-Superstore.csv')))  #这里改成文件名
pprint.pprint(data_super[0])

!pip install psycopg2
import psycopg2

def pgconnect():

    try: 
        conn = psycopg2.connect(host='localhost',   #登录信息
                                database='postgres',                                 
                                user='postgres',                                  
                                password='XXXXXX')
        print('connected')
    except Exception as e:
        print("unable to connect to the database")
        print(e)
        
pgconnect()

def pgexec( conn, sqlcmd, args, msg, silent=False ):
   """ utility function to execute some SQL statement
       can take optional arguments to fill in (dictionary)
       error and transaction handling built-in """
   retval = False
   with conn:
      with conn.cursor() as cur:
         try:
            if args is None:
               cur.execute(sqlcmd)
            else:
               cur.execute(sqlcmd, args)
            if silent == False: 
                print("success: " + msg)
            retval = True
         except Exception as e:
            if silent == False: 
                print("db error: ")
                print(e)
   return retval
   
   
conn = pgconnect()

#对应上
insert_stmt = """INSERT INTO Supermakert(order_id,order_date,ship_date,ship_mode,customer_id,customer_name,segment,country,city,state_name,postal_code,region,product_id,Category,sub_category,product_name,sales,quantity,discount,profit) VALUES (%(Order ID)s,%(Order Date)s,%(Ship Date)s,%(Ship Mode)s,%(Customer ID)s,%(Customer Name)s,%(Segment)s,%(Country)s,%(City)s,%(State)s,%(Postal Code)s,%(Region)s,%(Product ID)s,%(Category)s,%(Sub-Category)s,%(Product Name)s,%(Sales)s,%(Quantity)s,%(Discount)s,%(Profit)s
)"""
for row in data_super:
    pgexec (conn, insert_stmt, row, "row inserted")
