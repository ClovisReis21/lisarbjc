const { Kafka } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'loja-simulada',
  brokers: ['localhost:9092'],
});

async function Publicar(msgs = []) {
    const mensagens = [];
    msgs.map((el) => mensagens.push({value: el}));
    const producer = kafka.producer();
    await producer.connect();
    await producer.send({
        topic: 'vendas-deshboard-bronze',
        messages: mensagens,
    });
    await producer.disconnect();
}
Publicar(['Hello KafkaJS user!', 'Agra vai'])
module.exports = { Publicar }