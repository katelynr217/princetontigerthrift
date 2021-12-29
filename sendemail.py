#-----------------------------------------------------------------------
# sendemail.py
# Author: Katelyn Rodrigues, Brooke Gallagher
#-----------------------------------------------------------------------
import os
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, From

# send seller notification when a buyer has reserved their item 
# given seller and buyer info as dicts and item_name. 
# return true if successful or false if unsuccessful
def send_seller_reservation_notification(seller, buyer, item_name): 

    message = Mail(
        from_email= From('tigerthrift@princeton.edu', 'TigerThrift'),
        to_emails= seller['email']
    )

    message.dynamic_template_data = {
        'seller': seller['first_name'],
        'buyer': buyer['full_name'],
        'buyer_email': buyer['email'],
        'prodname': item_name,
        'buyer_phone': buyer['phone']
    }

    message.template_id = 'd-34e62704618448cd970bd0d8eb96f925'

    try:
        sg = SendGridAPIClient(os.environ.get('SENDGRID_API_KEY'))
        response = sg.send(message)
        print(response.status_code)
        print(response.body)
        print(response.headers)
    except Exception as e:
        print(str(e))
        return False

    return True

# send buyer notification when they have reserved an item 
# given seller and buyer info as dicts and item_name. 
# return true if successful or false if unsuccessful
def send_buyer_reservation_notification(seller, buyer, item_name): 

    message = Mail(
        from_email= From('tigerthrift@princeton.edu', 'TigerThrift'),
        to_emails= buyer['email']
    )

    message.dynamic_template_data = {
        'buyer': buyer['first_name'],
        'prodname': item_name,
        'seller_full_name': seller['full_name'],
        'seller_email': seller['email'],
        'seller_phone': seller['phone']
    }

    message.template_id = 'd-2ecc34fa76b540a8b41152f925dfcd01'

    try:
        sg = SendGridAPIClient(os.environ.get('SENDGRID_API_KEY'))
        response = sg.send(message)
        print(response.status_code)
        print(response.body)
        print(response.headers)
    except Exception as e:
        print(str(e))
        return False

    return True

# send buyer reminder when their item's reservation has <24hrs left to complete sale
# given seller and buyer info as dicts and item_name. 
# return true if successful or false if unsuccessful
def send_buyer_reservation_reminder(seller, buyer, item_name):
    message = Mail(
        from_email= From('tigerthrift@princeton.edu', 'TigerThrift'),
        to_emails= buyer['email']
    )

    message.dynamic_template_data = {
        'buyer': buyer['first_name'],
        'prodname': item_name,
        'seller': seller['full_name'],
        'Weblink': seller['email'],
        'seller_phone': seller['phone']
    }

    print("BUYER RESERVATION NOTIFICATION FOR ITEMNAME", item_name)
    print(str(message.dynamic_template_data))

    message.template_id = 'd-52bcd2394cbb4f63a70bc6e7c2c50645'

    try:
        sg = SendGridAPIClient(os.environ.get('SENDGRID_API_KEY'))
        response = sg.send(message)
        print(response.status_code)
        print(response.body)
        print(response.headers)
    
    except Exception as e:
        print(str(e))
        return False
    
    return True

# send seller reminder when one of their item's on reserve has <24hrs left to complete sale
# given seller and buyer info as dicts and item_name. 
# return true if successful or false if unsuccessful
def send_seller_reservation_reminder(seller, buyer, item_name):
    message = Mail(
        from_email= From('tigerthrift@princeton.edu', 'TigerThrift'),
        to_emails= seller['email']
    )

    message.dynamic_template_data = {
        'seller': seller['first_name'],
        'buyer': buyer['full_name'],
        'Weblink': buyer['email'],
        'prodname': item_name,
        'buyer_phone': buyer['phone']
    }

    print("SELLER RESERVATION NOTIFICATION FOR ITEMNAME", item_name)
    print(str(message.dynamic_template_data))

    message.template_id = 'd-08ff0c1398894d1bbbcb8e850a7e8080'

    try:
        sg = SendGridAPIClient(os.environ.get('SENDGRID_API_KEY'))
        response = sg.send(message)
        print(response.status_code)
        print(response.body)
        print(response.headers)
    
    except Exception as e:
        print(str(e))
        return False
    
    return True

# send buyer notification when their item's reservation period has expired
# given seller and buyer info as dicts and item_name. 
# return true if successful or false if unsuccessful
def send_buyer_expiration_notification(seller, buyer, item_name):
    message = Mail(
        from_email= From('tigerthrift@princeton.edu', 'TigerThrift'),
        to_emails= buyer['email']
    )

    message.dynamic_template_data = {
        'buyer': buyer['first_name'],
        'prodname': item_name,
        'seller': seller['full_name'],
        'Weblink': seller['email'],
        'seller_phone': seller['phone']
    }

    print("BUYER EXPIRATION NOTIFICATION FOR ITEMNAME", item_name)
    print(str(message.dynamic_template_data))

    message.template_id = 'd-875e76a6e7b542d5a578750cb44ab2a4'

    try:
        sg = SendGridAPIClient(os.environ.get('SENDGRID_API_KEY'))
        response = sg.send(message)
        print(response.status_code)
        print(response.body)
        print(response.headers)
    
    except Exception as e:
        print(str(e))
        return False

    return True

# send seller notification when one of their items on reserve has a reservation period that has expired
# given seller and buyer info as dicts and item_name. 
# return true if successful or false if unsuccessful
def send_seller_expiration_notification(seller, buyer, item_name):
    message = Mail(
        from_email= From('tigerthrift@princeton.edu', 'TigerThrift'),
        to_emails= seller['email']
    )

    message.dynamic_template_data = {
        'seller': seller['first_name'],
        'buyer': buyer['full_name'],
        'Weblink': buyer['email'],
        'prodname': item_name,
        'buyer_phone': buyer['phone']
    }
    print("SELLER EXPIRATION NOTIFICATION FOR ITEMNAME", item_name)
    print(str(message.dynamic_template_data))

    message.template_id = 'd-9647730514d94655bb99a0c408a989b4'

    try:
        sg = SendGridAPIClient(os.environ.get('SENDGRID_API_KEY'))
        response = sg.send(message)
        print(response.status_code)
        print(response.body)
        print(response.headers)
    
    except Exception as e:
        print(str(e))
        return False
    
    return True

# send seller notification if one of their selling items' reservations has been cancelled
# given seller and buyer info as dicts and item_name. 
# return true if successful or false if unsuccessful
def send_seller_cancellation(seller, buyer, item_name): 

    message = Mail(
        from_email= From('tigerthrift@princeton.edu', 'TigerThrift'),
        to_emails= seller['email']
    )

    message.dynamic_template_data = {
        'seller': seller['first_name'],
        'buyer': buyer['full_name'],
        'prodname': item_name
    }

    message.template_id = 'd-f7da6e2552fe4b4db07a1c9d7ac69c6c'

    try:
        sg = SendGridAPIClient(os.environ.get('SENDGRID_API_KEY'))
        response = sg.send(message)
        print(response.status_code)
        print(response.body)
        print(response.headers)
    
    except Exception as e:
        print(str(e))
        return False

    return True

# send buyer notification when they have cancelled a reservation
# given seller and buyer info as dicts and item_name. 
# return true if successful or false if unsuccessful
def send_buyer_cancellation(seller, buyer, item_name): 

    message = Mail(
        from_email= From('tigerthrift@princeton.edu', 'TigerThrift'),
        to_emails= buyer['email']
    )

    message.dynamic_template_data = {
        'buyer': buyer['first_name'],
        'prodname': item_name
    }

    message.template_id = 'd-e4185b77ade44789ac3897c6fe92ca70'

    try:
        sg = SendGridAPIClient(os.environ.get('SENDGRID_API_KEY'))
        response = sg.send(message)
        print(response.status_code)
        print(response.body)
        print(response.headers)
    except Exception as e:
        print(str(e))
        return False

    return True