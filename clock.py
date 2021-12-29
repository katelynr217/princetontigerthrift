#-----------------------------------------------------------------------
# clock.py
# Author: Katelyn Rodrigues, Brooke Gallagher
#-----------------------------------------------------------------------
import re
import os
from sys import stderr
from psycopg2 import connect
from contextlib import closing
from datetime import datetime
from datetime import timedelta
from sendemail import send_buyer_reservation_notification, send_seller_reservation_notification, send_buyer_reservation_reminder, send_seller_reservation_reminder, send_buyer_expiration_notification, send_seller_expiration_notification
from apscheduler.schedulers.background import BackgroundScheduler

# create background process apscheduler
sched = BackgroundScheduler({'apscheduler.timezone': 'UTC'})

# helper function to calculate the number of days and hours left to complete a reservation 
# given the reserved timestamp and current timestamp and send 'reservation about to expire' 
# and 'reservation expired' emails to buyers and sellers of such items given buyer and seller 
# info as dicts and item_name. return return formatted time left to complete reservation.
def days_between(d1, d2, seller, buyer, item_name):
    d1 = datetime.strptime(str(d1), "%Y-%m-%d %H:%M:%S")
    d2 = datetime.strptime(str(d2), "%Y-%m-%d %H:%M:%S")
    time_left = timedelta(days=3) - (d1-d2)
    time_split = (re.split('[ :]', str(time_left)))[0]
    print(str(time_split))
    if int(time_split) < 0:
        send_buyer_expiration_notification(seller, buyer, item_name)
        send_seller_expiration_notification(seller, buyer, item_name)
        return("YOUR RESERVATION HAS EXPIRED! 0 days left")
    left = str(time_left).split(':', 1)
    time_left = left[0]
    mins_secs_left = left[1]
    print("helloooo", str(time_left))
    if "day" not in str(time_left):
        hours_left = (str(time_left).split(', ', 1))[-1] # hours left on 0th day
        print("YOUR RESERVATION IS ABOUT TO EXPIRE! Only", str(hours_left), "hours left!")
        send_buyer_reservation_reminder(seller, buyer, item_name)
        send_seller_reservation_reminder(seller, buyer, item_name)
        return("YOUR RESERVATION WILL EXPIRE IN ", str(hours_left), " HOURS!")
    if time_left == str(0):
        mins_secs_left = mins_secs_left.replace(":", " minutes ")
        return(mins_secs_left, " seconds left")
    print("TIME LEFT:", time_left)
    print("Time Left Split:", (str(time_left).split(', ', 1))[-1])
    return(time_left, " hours left")

#-----------------------------------------------------------------------
# this scheduled function runs at 4pmET Monday-Sunday
#-----------------------------------------------------------------------

# iterates through each item in reservations table 
# to send reservation expiration reminder emails to item buyer and seller if necessary
# return true if job was successful, or return false if unsuccesful
@sched.scheduled_job('cron', day_of_week='0-6', hour=21) 
def scheduled_job():
    print('This job is run every day at 4pm EST.')
    DATABASE_URL = os.environ.get('DATABASE_URL')

    try:
        with connect (DATABASE_URL, sslmode='require') as connection:
            with closing(connection.cursor()) as cursor:

                stmt_str = 'SELECT * FROM reservations INNER JOIN items ON items.itemid = reservations.itemid AND items.sellernetid = reservations.sellernetid WHERE reservations.completedtime IS NULL;'
                cursor.execute(stmt_str)

                row = cursor.fetchone()

                list_of_items = []
                while row is not None:
                    item = {'itemid': row[0],
                    'buyernetid': row[1],
                    'sellernetid': row[2],
                    'reserved_time': row[3],
                    'status': row[17],
                    'prodname': row[19]}
                    list_of_items.append(item)
                    row = cursor.fetchone()

                for item in list_of_items:
                    # send buyer email
                    stmt_str = ('SELECT * from users where netid = %s')
                    cursor.execute(stmt_str, [item['buyernetid']])
                    row = cursor.fetchone()
                    buyer = {'netid': row[0],
                    'email': row[1],
                    'phone': row[3],
                    'first_name': row[4],
                    'last_name': row[5],
                    'full_name': row[6]}

                    stmt_str = ('SELECT * from users where netid = %s')
                    cursor.execute(stmt_str, [item['sellernetid']])
                    row = cursor.fetchone()
                    seller = {'netid': row[0],
                    'email': row[1],
                    'phone': row[3],
                    'first_name': row[4],
                    'last_name': row[5],
                    'full_name': row[6]}

                    if item['status'] != 1:
                        print("MISMATCH RESERVATION ITEM!!!")
                    # make sure to check in email template that if seller/buyer phone number is unknown then dont inlcude it email template
                    # get time stamp
                    f = '%Y-%m-%d %H:%M:%S'
                    now = datetime.utcnow()
                    dt = now.strftime(f)
                    days_between(dt, item['reserved_time'], seller, buyer, item['prodname'])

                return True

    except Exception as ex:
        print(ex, file=stderr)
        return False



sched.start()

while True:
    pass
