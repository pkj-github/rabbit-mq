import pika


def on_message_received(ch,method,properties,body):

    print(f'PKJ received the message :{body}')


connection_parameters=pika.ConnectionParameters('localhost')

connection=pika.BlockingConnection(connection_parameters)

channel=connection.channel()

channel.queue_declare(queue='pkj_queue')
#channel.queue_declare(queue='pkj_stream',durable=True,arguments={"x-queue-type": "stream"})

# Set the consumer QoS prefetch
channel.basic_qos(
  prefetch_count=100
)

#channel.basic_consume(queue='pkj_queue',auto_ack=True,on_message_callback=on_message_received)
channel.basic_consume(queue='pkj_queue',on_message_callback=on_message_received)

print('Started consuming')

channel.start_consuming()