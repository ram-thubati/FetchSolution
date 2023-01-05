import datetime
import hashlib
import json
import psycopg2
import localstack_client.session as boto3


class LoginMessage:

    def __init__(self,json_message, secret_salt):
        """
        Constructor creates an object of type LoginMessage with masked deviceid and ipaddress
        Parameters:
            json_message: flattened JSON of message's body
            secret_salt: a known secret salt to generate the hash for masking deviceid and ipaddress
        """
        self.user_id = json_message['user_id']
        self.device_type = json_message["device_type"]
        self.locale = json_message["locale"]
        
        #JSON message contain version number in a Major.Minor.Path format
        #Since the target type is int and not varchar, I chose to store on the
        #major version number.
        self.app_version = int(json_message["app_version"].partition('.')[0])
        
        #create_date defaults to current system date.
        self.create_date = datetime.datetime.now().date()

        #masking is achieved thorugh a SHA-256 hash with known SECRET_SALT.
        self.masked_ip = self.calculateHash(json_message["ip"],secret_salt)
        self.masked_device_id = self.calculateHash(json_message["device_id"], secret_salt)

    def getTuple(self):
        """
        returns a tuple containing all attributes of class LoginMessage
        """
        return (self.user_id, self.device_type, self.masked_ip, self.masked_device_id, self.locale, self.app_version, self.create_date)
    
    def calculateHash(self,str, secret_salt):
        """
        Retunrs the SHA 256 hash of string str
        Paramters:
            str: The string to hash
            secret_salt: a known salt. 
        """
        str_with_secret_salt = str + secret_salt
        hash_object = hashlib.sha256(str_with_secret_salt.encode('utf-8'))
        hex_digest = hash_object.hexdigest()
        return hex_digest


if __name__ == "__main__":
    # Create an SQS client
    sqs = boto3.client('sqs')

    #URL of the queue to read from
    QUEUE_URL = "http://localhost:4566/000000000000/login-queue"

    #in production this would be a symmetric key fetched from AWS KMS
    HASH_SECRET_SALT = "SECRET SALT"

    #infinite loop because a producers can send messages to Q at anytime.
    while(True):

        #tuples created from message batch are appended to this list.
        messages_to_insert = []

        # Read a batch of messages from the queue using long polling with wait time of 5 seconds.
        response = sqs.receive_message(QueueUrl=QUEUE_URL, MaxNumberOfMessages=16, WaitTimeSeconds= 5)

        if "Messages" in response:
            messages = response["Messages"]

            print("Received {} messages from Q at {}".format(len(messages), datetime.datetime.now()))

            #iterate over each message in the reponse and add tuple to list.
            for message in messages:
                message_body = json.loads(message["Body"])
                receipt_handle = message["ReceiptHandle"]
                
                try:
                    m = LoginMessage(message_body,HASH_SECRET_SALT)
                    messages_to_insert.append(m.getTuple())
                except:
                    print("Unknown message format found. Discarded message.")
                    continue
                finally:
                    #regardless of the message, it should be deleted from Q once its read.
                    sqs.delete_message(QueueUrl=QUEUE_URL, ReceiptHandle=receipt_handle)
            
            if(len(messages_to_insert) > 0):
                
                conn = psycopg2.connect(database='postgres', user='postgres',password='postgres',host='localhost',port='5432')

                cur = conn.cursor()
                insert_statement = "INSERT INTO user_logins (user_id, device_type, masked_ip, masked_device_id, locale, app_version, create_date) VALUES (%s, %s, %s, %s, %s, %s, %s)"

                #executemany to bulk insert.
                cur.executemany(insert_statement, messages_to_insert)

                conn.commit()

                # Close the cursor and connection
                cur.close()
                conn.close()

        #in a production system, this else block is absent.
        else:
            print("No more messages to read from Q. Program will now terminate.")
            break
