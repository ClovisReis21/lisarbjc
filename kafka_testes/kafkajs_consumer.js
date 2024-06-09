const { Kafka } = require('kafkajs');

const group = 'dashboards-consumer';

const kafka = new Kafka({
  clientId: group,
  brokers: ['localhost:9092'],
});

async function consumer() {
  const consumer = kafka.consumer({ groupId: group });
  await consumer.connect();

  await consumer.subscribe({ topics: ["teste-kafka"], fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      try{
        console.log(`ropic: ${topic} - partition: ${partition} - value: ${message.value.toString()}`)
      } catch(e) {
        console.log(`ERRO --> `, e, message.value.toString());
        return;
      }
    },
  });
}

consumer()