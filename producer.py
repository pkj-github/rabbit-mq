import pika
import api_code

connection_parameters=pika.ConnectionParameters('localhost')

connection=pika.BlockingConnection(connection_parameters)

channel=connection.channel()

#channel.queue_declare(queue='pkj_queue')
channel.queue_declare(queue='pkj_stream',durable=True,arguments={"x-queue-type": "stream"})

#message="{This: is Pankaj's first stream message}"

#channel.basic_publish(exchange='',routing_key='pkj_queue',body=str(api_code.crypto))
channel.basic_publish(exchange='',routing_key='pkj_stream',body=str(api_code.crypto))

#print('message sent : '+ str(api_code.crypto))
print('Stream message sent : ',str(api_code.crypto))

connection.close()
