from kafka import KafkaConsumer, TopicPartition
from json import loads, dump

def consume(topic, filename):
	consumer = KafkaConsumer(value_deserializer=lambda x: loads(x.decode('utf-8')), bootstrap_servers='104.248.248.196:9092,134.122.78.61:9092,134.209.225.2:9092',auto_offset_reset='earliest')
	tp = TopicPartition(topic, 0)
	consumer.assign([tp])
	consumer.seek_to_end(tp)             
	last_offset = consumer.position(tp)
	consumer.seek_to_beginning(tp)

	results = []
	for message in consumer:
		results.append(message.value)
		if message.offset == last_offset-1:
			break
	with open(filename, 'w+') as jf:
		dump(results, jf)

if __name__=='__main__':
	consume('US-meetups', 'US-meetups.json')
	consume('US-cities-every-minute', 'US-cities-every-minute.json')
	consume('Programming-meetups', 'Programming-meetups.json')

    

