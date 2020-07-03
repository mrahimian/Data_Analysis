#!/usr/bin/python

import psycopg2
#connect to database to recieve info
conn = psycopg2.connect(database="fpdb", user = "mohammadreza")


cur = conn.cursor()
# maximum sell of products in each province --> (province,produvt_id,has_sold)
# cur.execute('''select t.province,product_id,t.maximum from fp_stores_data,(select province , max(has_sold) as maximum from fp_stores_data group by province) as t''')
# rows = cur.fetchall()
# for row in rows:
#     print(row,"\n")

# city with maximum sell
# cur.execute('''select distinct city FROM fp_city_aggregation''')
# rows = cur.fetchall()
# max = 0
# city = " "
# for row in rows :
#     query="select has_sold from fp_city_aggregation where city=\'{}\'".format(row[0])
#     cur.execute(query)
#     res=cur.fetchall()
#     has_sold=res[0][0]
#     if has_sold > max:
#         max = has_sold
#         city = row
# print("{},{}".format(city,has_sold))

# market with maximum sell
cur.execute('''select distinct market_id FROM fp_store_aggregation''')
rows = cur.fetchall()
max = 0
city = " "
for row in rows :
    query="select has_sold from fp_store_aggregation where market_id=\'{}\'".format(row[0])
    cur.execute(query)
    res=cur.fetchall()
    has_sold=res[0][0]
    if has_sold > max:
        max = has_sold
        city = row
print("{},{}".format(city,has_sold))
# cur.execute('''select time,city,market_id,max(has_sold) FROM fp_store_aggregation group by time,city,market_id;''')
# rows = cur.fetchall()
# for row in rows:
#     print(row,"\n")

# cur.execute('''select t.city,t.m from (select city,max(price) as m FROM fp_stores_data group by city) as t,fp_stores_data; ''')
# rows = cur.fetchall()
# for row in rows:
#     print(row,"\n")    
# cur.execute('''select city,has_sold from fp_city_aggregation where has_sold = 580''')
# print(type(rows[1000]))
conn.commit()
conn.close()