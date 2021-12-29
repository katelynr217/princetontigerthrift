import re
import os
from sys import stderr
from psycopg2 import connect
from contextlib import closing
from datetime import datetime
from datetime import timedelta
from titlecase import titlecase


# add to users table if user is not already in the table (first time user)
# given dictionary of user_info containing netid
# return true if successful, return false if error
def add_user(user_info):
    DATABASE_URL = os.environ.get('DATABASE_URL')
    try:
        with connect (DATABASE_URL, sslmode='require') as connection:
            with closing(connection.cursor()) as cursor:
                # check if user is first time user, if so add to users table
                stmt_str = 'SELECT exists (SELECT 1 FROM users WHERE netid = %s LIMIT 1);'
                cursor.execute(stmt_str, [user_info['netid']])
                row = cursor.fetchone() # returned as tuple boolean
                is_user = row[0]
                print("ARE THEY ALREADY A USER???? " + str(is_user))
                # if new user, insert into users table
                if not is_user:
                    f = '%Y-%m-%d %H:%M:%S'
                    now = datetime.utcnow()
                    dt = now.strftime(f)
                    #print("started inserting into users table")
                    stmt_str = ('INSERT INTO users (netid, email, joined, phone, first_name, last_name, full_name) VALUES (%s, %s, %s, %s, %s, %s, %s)')
                    cursor.execute(stmt_str, [user_info['netid'], user_info['email'], dt, 'unknown', user_info['first_name'], user_info['last_name'], user_info['full_name']])
                    #print("finished inserting into users table")
                connection.commit()
                return True
    
    except Exception as ex:
       print(ex, file=stderr)
       return False


# based on netid, return user_info dictionary for a whitelisted user 
# containing netid, email, class_year, first, last, and full name
# or return False if error
def get_whitelist_user_info(netid):
    DATABASE_URL = os.environ.get('DATABASE_URL')
    try:
        with connect (DATABASE_URL, sslmode='require') as connection:
            with closing(connection.cursor()) as cursor:
                stmt_str = 'SELECT * FROM users WHERE netid=%s;'
                cursor.execute(stmt_str, [netid])
                row = cursor.fetchone() # returned as tuple boolean
                user_info = {'first_name': row[4],
                'last_name': row[5],
                'full_name': row[6],
                'netid': netid,
                'email': row[1],
                'class_year': 'faculty'}
                user_info['phone'] = get_user_phone(netid) # will either be unknown or the phone number itself
                connection.commit()
                return user_info 
    
    except Exception as ex:
       print(ex, file=stderr)
       return False

# return the phone number from users table which will either be "unknown" 
# or the number itself given netid, or return False if error
def get_user_phone(netid):
    DATABASE_URL = os.environ.get('DATABASE_URL')
    try:
        with connect (DATABASE_URL, sslmode='require') as connection:
            with closing(connection.cursor()) as cursor:
                stmt_str = 'SELECT phone FROM users WHERE netid=%s;'
                cursor.execute(stmt_str, [netid])
                row = cursor.fetchone()
                phone_number = row[0]
                connection.commit()
                return phone_number # will either be unknown or the phone number itself
    
    except Exception as ex:
       print(ex, file=stderr)
       return False

# add phone number to users table if it is different than what is currently in the table
# return the new phone number or return false if error
def add_user_phone(netid, phone_number):
    DATABASE_URL = os.environ.get('DATABASE_URL')
    try:
        with connect (DATABASE_URL, sslmode='require') as connection:
            with closing(connection.cursor()) as cursor:
                stmt_str = 'UPDATE users SET phone=%s WHERE netid=%s;'
                cursor.execute(stmt_str, [phone_number, netid])
                print("updated phone number in database")
                connection.commit()
                return phone_number # will either be None or the phone number itself
    
    except Exception as ex:
       print(ex, file=stderr)
       return False

# reserve an item by changing status in items table and adding to reservations table given an itemid and buyernetid
# return the error or false if error, or 
# return sellernetid, seller_first_name, seller_full_name, seller_email, seller_phone, and item name if successful
def reserve_item(buyernetid, itemid):
    DATABASE_URL = os.environ.get('DATABASE_URL')
    try:
        with connect (DATABASE_URL, sslmode='require') as connection:
            with closing(connection.cursor()) as cursor:
                    f = '%Y-%m-%d %H:%M:%S'
                    now = datetime.utcnow()
                    dt = now.strftime(f)

                    # change status in items table
                    stmt_str = ('SELECT status, prodname, sellernetid from items where itemid = %s')
                    cursor.execute(stmt_str, [itemid])
                    row = cursor.fetchone()
                    if row is None and len(row) != 3:
                        raise Exception("cannot find item info")
                    currentstatus = row[0]
                    prodname = row[1]
                    sellernetid = row[2]
                    if currentstatus == 1:
                        raise Exception("item already reserved")
                    if currentstatus != 0:
                        raise Exception("item unavailable for reservation")
                    if sellernetid is None:
                        raise Exception("cannot find sellerid")
                        
                    # insert into reservations table
                    stmt_str = ('INSERT INTO reservations (itemid, buyernetid, sellernetid, reservedtime) VALUES (%s, %s, %s, %s)')
                    cursor.execute(stmt_str, [itemid, buyernetid, sellernetid, dt])

                    print ("inserted into reservations table")
                    stmt_str = ('UPDATE items set status = 1 where itemid = %s')
                    cursor.execute(stmt_str, [itemid])
                    print("updated reservation status in items table")

                    stmt_str = ('SELECT first_name, full_name, email, phone from users where netid = %s')
                    cursor.execute(stmt_str, [sellernetid])
                    row = cursor.fetchone()
                    seller_first_name = row[0]
                    seller_full_name = row[1]
                    seller_email = row[2]
                    seller_phone = row[3]
                    connection.commit()
                    return sellernetid, seller_first_name, seller_full_name, seller_email, seller_phone, titlecase(str(prodname))
    
    except Exception as ex:
       print(ex, file=stderr)
       if str(ex) == "item already reserved" or str(ex) == "cannot find item info" or str(ex) == "item unavailable for reservation" or str(ex) == "cannot find sellerid":
           return str(ex)
       return False
    

# update items, users, and sellers tables when user uploads an item to sell
# given item as a dictionary with its details, and netid from user_info dictionary.
# return true if successful or false if error 
def add_item(item, user_info):
    DATABASE_URL = os.environ.get('DATABASE_URL')

    try:
        with connect (DATABASE_URL, sslmode='require') as connection:
            with closing(connection.cursor()) as cursor:
                
                # get time stamp
                f = '%Y-%m-%d %H:%M:%S'
                now = datetime.utcnow()
                dt = now.strftime(f)

                add_user(user_info)
                
                # insert item into items table
                stmt_str = ('INSERT INTO items '
                + '(type, subtype, size, gender, price, priceflexibility, color, condition, brand, "desc", posted, photolink, status, sellernetid, prodname, photolink1, photolink2, photolink3) ' +
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 0, %s, %s, %s, %s, %s)")
                cursor.execute(stmt_str, [item['type'], item['subtype'], item['size'], item['gender'], item['price'], item['priceflexibility'], item['color'], item['condition'], item['brand'], item['desc'], dt, item['photolink'], user_info['netid'], item['prodname'], item['photolink1'], item['photolink2'], item['photolink3']])                # get most recent itemid inserted (item id of currently inserted item)
                stmt_str = 'SELECT last_value FROM items_itemid_seq;'
                cursor.execute(stmt_str)
                row = cursor.fetchone()
                recent_item_id = row[0]
                print("LAST INSERTED INDEX: " + str(recent_item_id))
                # insert into sellers table 
                stmt_str = ('INSERT INTO sellers '
                + '(netid, itemid) ' +
                "VALUES (%s, %s)")
                cursor.execute(stmt_str, [user_info['netid'], str(recent_item_id)])

                connection.commit()
                return True

    except Exception as ex:
       print(ex, file=stderr)
       return False

# given new item details stored in item dict and netid from user_info dict,
# update users, and items table to edit an item.
# return true if successful or false if unsuccessful
def edit_item_db(item, user_info):
    DATABASE_URL = os.environ.get('DATABASE_URL')

    try:
        with connect (DATABASE_URL, sslmode='require') as connection:
            with closing(connection.cursor()) as cursor:
                print("entered edit item function!!!")
                # get time stamp
                f = '%Y-%m-%d %H:%M:%S'
                now = datetime.utcnow()
                dt = now.strftime(f)
                add_user(user_info)
                print("new item info!!",str(item))
                # insert item into items table
                stmt_str = ('UPDATE items SET type=%s, subtype=%s, size=%s, gender=%s, price=%s, priceflexibility=%s, color=%s, condition=%s, brand=%s, "desc"=%s, posted=%s, status=0, sellernetid=%s, prodname=%s, photolink=%s, photolink1=%s, photolink2=%s, photolink3=%s WHERE itemid=%s;')
                cursor.execute(stmt_str, [item['type'], item['subtype'], item['size'], item['gender'], item['price'], item['priceflexibility'], item['color'], item['condition'], item['brand'], item['desc'], dt, user_info['netid'], item['prodname'], item['photolink'], item['photolink1'], item['photolink2'], item['photolink3'], item['itemid']])
                print("updated item details in database!")

                connection.commit()
                return True

    except Exception as ex:
       print(ex, file=stderr)
       return False

# return item details given an itemid, or false if unsuccessful
def item_details(itemid):
    DATABASE_URL = os.environ.get('DATABASE_URL')

    try:
        with connect (DATABASE_URL, sslmode='require') as connection:
            with closing(connection.cursor()) as cursor:

                stmt_str = "SELECT * from items where itemid = %s"
                cursor.execute(stmt_str, [itemid])

                row = cursor.fetchone()

                if row is None:
                    return None

                item = {'itemid': row[0],
                    'type': row[1],
                    'subtype': row[2],
                    'desc': row[9],
                    'gender': row[4],
                    'price': row[5],
                    'priceflexibility': row[18],
                    'size': row[3],
                    'brand': row[8],
                    'condition': row[7],
                    'color': row[6],
                    'timestamp': row[10],
                    'photolink': row[11],
                    'status': row[12],
                    'sellernetid': row[13],
                    'prodname': titlecase(str(row[14])),
                    'photolink1': row[15],
                    'photolink2': row[16],
                    'photolink3': row[17]
                    }
                if item['status'] == 1:
                    stmt_str = "SELECT buyernetid from reservations where itemid = %s"
                    cursor.execute(stmt_str, [itemid])
                    item['buyernetid'] = str(cursor.fetchone()[0])

                return item

    except Exception as ex:
       print(ex, file=stderr)
       return False

# delete a reservation by updating reservations and items tables given itemid 
# return True if successful or false if unsuccessful
def delete_reserve(itemid):
    DATABASE_URL = os.environ.get('DATABASE_URL')

    try:
        with connect (DATABASE_URL, sslmode='require') as connection:
            with closing(connection.cursor()) as cursor:

                stmt_str = "DELETE FROM reservations where itemid = %s"
                cursor.execute(stmt_str, [itemid])
                print("deleted from reservations")

                stmt_str = "UPDATE items SET status=0 WHERE itemid= %s"
                cursor.execute(stmt_str, [itemid])
                print("updated items table")

                connection.commit()
                return True

    except Exception as ex:
       print(ex, file=stderr)
       return False

# return dict with seller details and item name given itemid 
# return false if unsuccessful
def get_seller_and_item_info(itemid):
    DATABASE_URL = os.environ.get('DATABASE_URL')

    try:
        with connect (DATABASE_URL, sslmode='require') as connection:
            with closing(connection.cursor()) as cursor:

                stmt_str = "SELECT sellernetid, prodname FROM items WHERE itemid=%s;"
                cursor.execute(stmt_str, [itemid])
                info = cursor.fetchone()
                sellernetid = info[0]
                item_name = info[1]
                stmt_str = "SELECT first_name, full_name, email FROM users WHERE netid=%s;"
                cursor.execute(stmt_str, [sellernetid])
                row = cursor.fetchone()
                seller_first_name = row[0]
                seller_full_name = row[1]
                seller_email = row[2]
                seller = {'first_name': seller_first_name,
                'full_name': seller_full_name,
                'email': seller_email}
                return (seller, item_name)
    except Exception as ex:
       print(ex, file=stderr)
       return False

# complete a reservation given itemid by updating items and reservations tables
# return true if successful or false or error if unsuccessful
def complete_reserve(itemid):
    DATABASE_URL = os.environ.get('DATABASE_URL')

    try:
        with connect (DATABASE_URL, sslmode='require') as connection:
            with closing(connection.cursor()) as cursor:

                stmt_str = "SELECT status FROM items WHERE itemid=%s;"
                cursor.execute(stmt_str, [itemid])
                if cursor.fetchone()[0] != 1:
                    raise Exception("item status is not reserved")

                # get time stamp
                f = '%Y-%m-%d %H:%M:%S'
                now = datetime.utcnow()
                dt = now.strftime(f)


                stmt_str = "UPDATE reservations SET completedtime = %s WHERE itemid = %s"
                cursor.execute(stmt_str, [dt, itemid])
                print("completed reservations from reservations")

                stmt_str = "UPDATE items SET status=2 WHERE itemid= %s"
                cursor.execute(stmt_str, [itemid])
                print("updated items table")

                connection.commit()
                return True

    except Exception as ex:
       print(ex, file=stderr)
       if str(ex) == "item status is not reserved":
           return str(ex)
       return False

# helper function to calculate the number of days and hours left 
# to complete a reservation given the reserved timestamp and current timestamp
# return formatted time left
def days_between(d1, d2):
    d1 = datetime.strptime(str(d1), "%Y-%m-%d %H:%M:%S")
    d2 = datetime.strptime(str(d2), "%Y-%m-%d %H:%M:%S")
    time_left = timedelta(days=3) - (d1-d2)
    time_split = (re.split('[ :]', str(time_left)))[0]
    print(str(time_split))
    if int(time_split) < 0:
        return("YOUR RESERVATION HAS EXPIRED! 0 days left")
    left = str(time_left).split(':', 1)
    time_left = left[0]
    mins_secs_left = left[1]
    print("helloooo", str(time_left))
    if "day" not in str(time_left):
        hours_left = (str(time_left).split(', ', 1))[-1] # hours left on 0th day
        print("YOUR RESERVATION IS ABOUT TO EXPIRE! Only", str(hours_left), "hours left!")
        return("YOUR RESERVATION WILL EXPIRE IN ", str(hours_left), " HOURS!")
    if time_left == str(0):
        mins_secs_left = mins_secs_left.replace(":", " minutes ")
        return(mins_secs_left, " seconds left")
    print("TIME LEFT:", time_left)
    print("Time Left Split:", (str(time_left).split(', ', 1))[-1])
    return(time_left, " hours left")

# return a list of dictionaries containing each currently active (not sold or reserved) item and its details 
# given a seller's netid obtained from user_info, return false if unsuccessful
def curr_active_items(user_info):
    DATABASE_URL = os.environ.get('DATABASE_URL')
    results = []
    try:
        with connect (DATABASE_URL, sslmode='require') as connection:
            with closing(connection.cursor()) as cursor:

                stmt_str = 'SELECT * FROM items WHERE sellernetid = %s and status = 0'
                cursor.execute(stmt_str, [user_info['netid']])
                item_info = cursor.fetchone()
                while item_info is not None:
                    item = {'itemid': item_info[0],
                    'type': item_info[1],
                    'subtype': item_info[2],
                    'desc': item_info[9],
                    'gender': item_info[4],
                    'price': item_info[5],
                    'size': item_info[3],
                    'brand': item_info[8],
                    'condition': item_info[7],
                    'color': item_info[6],
                    'timestamp': item_info[10],
                    'photolink': item_info[11],
                    'status': item_info[12],
                    'sellernetid': item_info[13],
                    'prodname': titlecase(str(item_info[14])),
                    }
                    item_info = cursor.fetchone()
                    results.append(item)
                return results

    except Exception as ex:
        print(ex, file=stderr)
        return False

# return buyernetid and buyer_full_name for a reserved item given its itemid
# return false if unsuccessful
def reserved_netid(itemid):
    DATABASE_URL = os.environ.get('DATABASE_URL')
    buyernetid = ""
    buyer_full_name = ""
    try:
        with connect (DATABASE_URL, sslmode='require') as connection:
            with closing(connection.cursor()) as cursor:

                stmt_str = 'SELECT buyernetid FROM reservations WHERE completedtime IS NULL AND itemid = %s;'
                cursor.execute(stmt_str, [itemid])

                row = cursor.fetchone()

                if row is not None:
                    buyernetid = row[0]

                stmt_str = 'SELECT full_name FROM users WHERE netid = %s;'
                cursor.execute(stmt_str, [str(buyernetid)])

                row = cursor.fetchone()
                
                if row is not None:
                    buyer_full_name = row[0]

                return (str(buyernetid), str(buyer_full_name))

    except Exception as ex:
        print(ex, file=stderr)
        return False
    
# return buyernetid and buyer_full_name for a sold item given its itemid
# return false if unsuccessful
def bought_netid(itemid):
    DATABASE_URL = os.environ.get('DATABASE_URL')
    buyernetid = ""
    buyer_full_name = ""
    try:
        with connect (DATABASE_URL, sslmode='require') as connection:
            with closing(connection.cursor()) as cursor:

                stmt_str = 'SELECT buyernetid FROM reservations WHERE completedtime IS NOT NULL AND itemid = %s;'
                cursor.execute(stmt_str, [itemid])
                row = cursor.fetchone()

                if row is not None:
                    buyernetid = row[0]

                stmt_str = 'SELECT full_name FROM users WHERE netid = %s;'
                cursor.execute(stmt_str, [str(buyernetid)])

                row = cursor.fetchone()

                if row is not None:
                    buyer_full_name  = row[0]
                return (str(buyernetid), str(buyer_full_name))
    
    except Exception as ex:
        print(ex, file=stderr)
        return False

# return all the items a user has reserved as a list of dicts containing each item's details, 
# reservation details, and the item's seller details given a user's netid 
# obtained from user_info dict, return false if unsuccessful
def reserved_items(user_info):
    DATABASE_URL = os.environ.get('DATABASE_URL')

    try:
        with connect (DATABASE_URL, sslmode='require') as connection:
            with closing(connection.cursor()) as cursor:

                stmt_str = 'SELECT * FROM reservations WHERE completedtime IS NULL AND buyernetid = %s;'
                cursor.execute(stmt_str, [user_info['netid']])

                row = cursor.fetchone()

                item_ids = {}
                results = []
                while row is not None:
                    itemid = row[0]
                    reserved_time = row[3]
                    item_ids[itemid] = reserved_time
                    row = cursor.fetchone()

                for item_id in item_ids:
                    stmt_str = ('SELECT * from items where itemid = %s')
                    cursor.execute(stmt_str, [item_id])
                    item_info = cursor.fetchone()
                    # get time stamp
                    f = '%Y-%m-%d %H:%M:%S'
                    now = datetime.utcnow()
                    dt = now.strftime(f)
                    time_left_to_complete_reservation = days_between(dt, item_ids[item_id])
                    reservation_time_left = ''.join(time_left_to_complete_reservation)
                    item = {'itemid': item_info[0],
                    'type': item_info[1],
                    'subtype': item_info[2],
                    'desc': item_info[9],
                    'gender': item_info[4],
                    'price': item_info[5],
                    'size': item_info[3],
                    'brand': item_info[8],
                    'condition': item_info[7],
                    'color': item_info[6],
                    'timestamp': item_info[10],
                    'photolink': item_info[11],
                    'status': item_info[12],
                    'sellernetid': item_info[13],
                    'prodname': titlecase(str(item_info[14])),
                    'reservation_time_left': str(reservation_time_left)
                    }
                    stmt_str = ('SELECT * from users where netid = %s')
                    cursor.execute(stmt_str, [item['sellernetid']])
                    seller_info = cursor.fetchone()
                    seller_phone = seller_info[3]
                    seller_full_name = seller_info[6]
                    item['seller_full_name'] = seller_full_name
                    item['seller_phone'] = seller_phone
                    if item['status'] == 1:
                        results.append(item)
                    # error if item in reservation table is not marked as reserved in items table
                    if item['status'] != 1:
                        print("MISMATCH RESERVATION ITEM!!!")
                return results

    except Exception as ex:
        print(ex, file=stderr)
        return False
    
# return items that the user is selling and have been reserved by others as a 
# list of dicts each containing the item details, reservation details, and 
# its buyer details return false if unsuccessful
def seller_reservations(user_info):
    DATABASE_URL = os.environ.get('DATABASE_URL')

    try:
        with connect (DATABASE_URL, sslmode='require') as connection:
            with closing(connection.cursor()) as cursor:

                stmt_str = 'SELECT * FROM reservations INNER JOIN items ON items.itemid = reservations.itemid AND items.sellernetid = reservations.sellernetid where reservations.completedtime IS NULL and reservations.sellernetid=%s ORDER BY reservations.reservedtime ASC;'
                cursor.execute(stmt_str, [user_info['netid']])

                row = cursor.fetchone()
                results = []
                while row is not None:
                    reserved_time = row[3]
                    # get time stamp
                    f = '%Y-%m-%d %H:%M:%S'
                    now = datetime.utcnow()
                    dt = now.strftime(f)
                    time_left_to_complete_reservation = days_between(dt, reserved_time)
                    reservation_time_left = ''.join(time_left_to_complete_reservation)
                    item={'itemid': row[0],
                    'buyernetid': row[1],
                    'sellernetid': row[2],
                    'reservedtime': row[3],
                    'completedtime': row[4],
                    'type': row[6],
                    'subtype': row[7],
                    'desc': row[14],
                    'gender': row[9],
                    'price': row[10],
                    'size': row[8],
                    'brand': row[13],
                    'condition': row[12],
                    'color': row[11],
                    'timestamp': row[15],
                    'photolink': row[16],
                    'status': row[17],
                    'prodname': titlecase(str(row[19])),
                    'reservation_time_left': str(reservation_time_left)}
                    if item['status'] == 1:
                        results.append(item)
                    elif item['status'] != 1:
                        print("ERROR: reserved item not marked as reserved!")
                    row = cursor.fetchone()
                for item in results:
                    stmt_str = ('SELECT * from users where netid = %s')
                    cursor.execute(stmt_str, [item['buyernetid']])
                    item['buyer_full_name'] = cursor.fetchone()[6]

                return results

    except Exception as ex:
        print(ex, file=stderr)
        return False

# return a list of dicts each containing an item's details, completed-sale date, 
# and buyer details for items the user has sold in the past given the user's
# netid obtained from user_info. return false if unsuccessful.
def items_sold_in_past(user_info):
    DATABASE_URL = os.environ.get('DATABASE_URL')

    try:
        with connect (DATABASE_URL, sslmode='require') as connection:
            with closing(connection.cursor()) as cursor:

                stmt_str = 'SELECT * FROM reservations WHERE completedtime IS NOT NULL AND sellernetid = %s order by completedtime desc;'
                cursor.execute(stmt_str, [user_info['netid']])

                row = cursor.fetchone()

                item_ids = {}
                results = []
                while row is not None:
                    itemid = row[0]
                    completed_time = row[4]
                    item_ids[itemid] = completed_time
                    row = cursor.fetchone()

                for item_id in item_ids:
                    stmt_str = ('SELECT * from items where itemid = %s')
                    cursor.execute(stmt_str, [item_id])
                    item_info = cursor.fetchone()
                    purchased_date = datetime.strptime(str(item_ids[item_id]), "%Y-%m-%d %H:%M:%S")
                    purchased_date = (str(purchased_date).split(' ', 1))[0]
                    item = {'itemid': item_info[0],
                    'type': item_info[1],
                    'subtype': item_info[2],
                    'desc': item_info[9],
                    'gender': item_info[4],
                    'price': item_info[5],
                    'size': item_info[3],
                    'brand': item_info[8],
                    'condition': item_info[7],
                    'color': item_info[6],
                    'timestamp': item_info[10],
                    'photolink': item_info[11],
                    'status': item_info[12],
                    'sellernetid': item_info[13],
                    'prodname': titlecase(str(item_info[14])),
                    'purchase_completed': str(purchased_date)
                    }
                    stmt_str = ('SELECT buyernetid from reservations where itemid = %s')
                    cursor.execute(stmt_str, [item['itemid']])
                    buyernetid = cursor.fetchone()[0]
                    item['buyernetid'] = buyernetid
                    stmt_str = ('SELECT full_name from users where netid = %s')
                    cursor.execute(stmt_str, [item['buyernetid']])
                    item['buyer_full_name'] = cursor.fetchone()[0]
                    # error if item in reservation table is not marked as reserved in items table
                    if item['status'] == 2:
                        results.append(item)
                        print("appended item")
                    elif item['status'] != 2:
                        print("ERROR!! completed reservation not marked as status 2")

                return results

    except Exception as ex:
        print(ex, file=stderr)
        return False

# return a list of dicts each containing an item's details, completed-sale date, 
# and seller details for items the user has purchased given the user's netid obtained from user_info
# return false if unsuccessful.
def past_purchases(user_info):
    DATABASE_URL = os.environ.get('DATABASE_URL')

    try:
        with connect (DATABASE_URL, sslmode='require') as connection:
            with closing(connection.cursor()) as cursor:

                stmt_str = 'SELECT * FROM reservations WHERE completedtime IS NOT NULL AND buyernetid = %s;'
                cursor.execute(stmt_str, [user_info['netid']])

                row = cursor.fetchone()

                item_ids = {}
                results = []
                while row is not None:
                    itemid = row[0]
                    completed_time = row[4]
                    item_ids[itemid] = completed_time
                    row = cursor.fetchone()

                for item_id in item_ids:
                    stmt_str = ('SELECT * from items where itemid = %s')
                    cursor.execute(stmt_str, [item_id])
                    item_info = cursor.fetchone()
                    purchased_date = datetime.strptime(str(item_ids[item_id]), "%Y-%m-%d %H:%M:%S")
                    purchased_date = (str(purchased_date).split(' ', 1))[0]
                    item = {'itemid': item_info[0],
                    'type': item_info[1],
                    'subtype': item_info[2],
                    'desc': item_info[9],
                    'gender': item_info[4],
                    'price': item_info[5],
                    'size': item_info[3],
                    'brand': item_info[8],
                    'condition': item_info[7],
                    'color': item_info[6],
                    'timestamp': item_info[10],
                    'photolink': item_info[11],
                    'status': item_info[12],
                    'sellernetid': item_info[13],
                    'prodname': titlecase(str(item_info[14])),
                    'purchase_completed': str(purchased_date)
                    }
                    stmt_str = ('SELECT full_name from users where netid=%s;')
                    cursor.execute(stmt_str, [item['sellernetid']])
                    item['seller_full_name'] = cursor.fetchone()[0]
                    # error if item in reservation table is not marked as reserved in items table
                    if item['status'] == 2:
                        results.append(item)
                    
                return results

    except Exception as ex:
        print(ex, file=stderr)
        return False

# return a list of all distinct brands among all active items
def all_brands():
    DATABASE_URL = os.environ.get('DATABASE_URL')
    brands = []
    try:
        with connect (DATABASE_URL, sslmode='require') as connection:
            with closing(connection.cursor()) as cursor:
                stmt_str = "SELECT DISTINCT brand from items where status = 0"
                cursor.execute(stmt_str)
                row = cursor.fetchone()

                while row is not None:
                    brands.append(row[0])
                    row = cursor.fetchone()

    except Exception as ex:
        print(ex, file=stderr)
        return brands

    return brands

# return a list of dicts each containing an active (not sold or reserved) item's details 
# given filters and orders to sort by, return false if unsuccessful
def search_items(search, filter, sort):
    DATABASE_URL = os.environ.get('DATABASE_URL')
    search_results = []

    try:
        with connect (DATABASE_URL, sslmode='require') as connection:
            with closing(connection.cursor()) as cursor:
                stmt_str = "SELECT * from items "
                cmd_args = []
            
                stmt_str += "where upper(prodname) LIKE upper(%s) AND status = 0"

                if search is None:
                    search = ""

                cmd_args.append("%" + search + "%")

                if filter:
                    if 'type' in filter and filter['type'] is not None and filter["type"] != '':
                        print("entered type if")
                        stmt_str += "AND type = %s "
                        cmd_args.append(filter['type'])
                    if 'subtype' in filter and filter['subtype'] is not None and filter["subtype"] != '':
                        stmt_str += "AND subtype = %s "
                        cmd_args.append(filter['subtype'])
                    if 'size' in filter and filter['size'] is not None and filter["size"] != '':
                        stmt_str += "AND size = %s "
                        cmd_args.append(filter['size'])
                    if 'gender' in filter and filter['gender'] is not None and filter["gender"] != '':
                        stmt_str += "AND gender = %s "
                        cmd_args.append(filter['gender'])
                    if 'brand' in filter and filter['brand'] is not None and filter["brand"] != '':
                        stmt_str += "AND brand = %s "
                        cmd_args.append(filter['brand'])
                    if 'condition' in filter and filter['condition'] is not None and filter["condition"] != '':
                        stmt_str += "AND condition = %s "
                        cmd_args.append(filter['condition'])
                    if 'color' in filter and filter['color'] is not None and filter["color"] != '':
                        stmt_str += "AND color = %s "
                        cmd_args.append(filter['color'])
          
                # change order by when sort by is in place
                if sort:
                    if sort == "newest to oldest":
                        stmt_str += "ORDER BY posted desc"
                    if sort == "oldest to newest":
                        stmt_str += "ORDER BY posted asc"
                    if sort == "price low to high":
                        stmt_str += "ORDER BY price asc"
                    if sort == "price high to low":
                        stmt_str += "ORDER BY price desc"
                else:
                    stmt_str += "ORDER BY itemid asc"

                cursor.execute(stmt_str, cmd_args)

                row = cursor.fetchone()
                results = []

                while row is not None:
                    item = {'itemid': row[0],
                    'type': row[1],
                    'subtype': row[2],
                    'desc': row[9],
                    'gender': row[4],
                    'price': row[5],
                    'size': row[3],
                    'brand': row[8],
                    'condition': row[7],
                    'color': row[6],
                    'timestamp': row[10],
                    'photolink': row[11],
                    'status': row[12],
                    'sellernetid': row[13],
                    'prodname': titlecase(str(row[14])),
                    'photolink1': row[15],
                    'photolink2': row[16],
                    'photolink3': row[17],
                    }
                    results.append(item)
                    row = cursor.fetchone()

    except Exception as ex:
        print(ex, file=stderr)
        return False

    print(str(len(results)) + " items")
    return results

# delete an item from all tables given itemid
# return true if successful, or false if unsuccessful
def remove_item(itemid):
    DATABASE_URL = os.environ.get('DATABASE_URL')

    try:
        with connect (DATABASE_URL, sslmode='require') as connection:
            with closing(connection.cursor()) as cursor:

                # error handling
                stmt_str="SELECT status FROM items WHERE itemid=%s;"
                cursor.execute(stmt_str, [itemid])
                result = cursor.fetchone()
                if result is None:
                    raise Exception("item does not exist")
                item_status = result[0]
                if item_status != 0:
                    raise Exception("item cannot be deleted")

                stmt_str = ('DELETE FROM items WHERE itemid=%s;')
                cursor.execute(stmt_str, [itemid])
                stmt_str = 'DELETE FROM sellers WHERE itemid=%s;'
                cursor.execute(stmt_str, [itemid])
                print("itemid", itemid, "was deleted")
                connection.commit()
                return True

    except Exception as ex:
        print(ex, file=stderr)
        if str(ex) == "item does not exist" or str(ex) == "item cannot be deleted":
            return str(ex)
        return False

    
