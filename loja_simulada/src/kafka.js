const { Kafka, CompressionTypes } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'loja-simulada',
  brokers: ['localhost:9092'],
});

async function Publicar(msgs = []) {
    const mensagens = [];
    msgs.map((el) => mensagens.push({value: el}));
    const producer = kafka.producer({
      transactionalId: 'loja-simulada',
      maxInFlightRequests: 1,
      idempotent: true
    });
    await producer.connect();
    await producer.send({
        topic: 'vendas-deshboard-bronze',
        acks: -1,
        timeout: 60000,
        compression: CompressionTypes.GZIP,
        messages: mensagens,
    });
    await producer.disconnect();
}

module.exports = { Publicar }