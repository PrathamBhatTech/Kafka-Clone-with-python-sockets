import sys
from json import dumps

from myKafka import KafkaConsumer

bootstrap_servers = ['localhost:9092']
topicName = sys.argv[1]
consumer = KafkaConsumer (
    topicName,
    bootstrap_servers = bootstrap_servers,
    auto_offset_reset = 'earliest',
    value_deserializer = lambda x: x.decode('utf-8')
)

data = []
result = {}
states = []

try:
    for line in consumer:
        line = line.value
        
        if line == "EOF\n":
            break

        else:
            try:
                state = line.split(',', 1)[0]
                _, min_, max_, _ = line.replace('\n', '').rsplit(',', 3)
                
                data.append([state, min_, max_])
                states.append(state)
            except Exception as e:
                pass

    states = [*set(states)]
    states.sort()

    for state in states:
        
        min_sum = max_sum = n = 0
        
        for line in data:
            if line[0] == state:
                n += 1
                min_sum += float(line[1])
                max_sum += float(line[2])

        min_avg = round(min_sum/n, 2)
        max_avg = round(max_sum/n, 2)

        result[state] = {
            "Min":min_avg,
            "Max":max_avg
        }
        
    print(dumps(result, indent=4), end="")

except KeyboardInterrupt:
    sys.exit()
