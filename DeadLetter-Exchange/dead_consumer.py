import pika
from pika.exchange_type import ExchangeType

def deadletter_queue_on_message_received(ch, method, properties, body):
    print(f'Dead letter - received new message: {body}')
    ch.basic_ack(method.delivery_tag)

def main_queue_on_message_received(ch, method, properties, body):
    print(f'Main - received new message: {body}')

connection_parameters = pika.ConnectionParameters('localhost')

connection = pika.BlockingConnection(connection_parameters)

channel = connection.channel()

channel.exchange_declare(
    exchange='mainexchange', 
    exchange_type=ExchangeType.direct)

channel.exchange_declare(
    exchange='dlx',
    exchange_type=ExchangeType.fanout
)

channel.queue_declare(
    queue='mainexchangequeue', 
    arguments={'x-dead-letter-exchange': 'dlx',  'x-message-ttl' : 10000})
channel.queue_bind('mainexchangequeue', 'mainexchange', 'test')

channel.basic_consume(queue='mainexchangequeue',auto_ack=True,on_message_callback=main_queue_on_message_received)

channel.queue_declare('deadletterqueue')
channel.queue_bind('deadletterqueue', 'dlx')
channel.basic_consume(queue='deadletterqueue', on_message_callback=deadletter_queue_on_message_received)

print('Starting Consuming')

channel.start_consuming()