const { Kafka, CompressionTypes, Partitioners } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'loja-simulada',
  brokers: ['localhost:9092'],
});

async function Publicar(msgs = []) {
    const mensagens = [];
    msgs.map((el) => mensagens.push({value: el}));
    const producer = kafka.producer({
      createPartitioner: Partitioners.LegacyPartitioner,
      transactionalId: 'loja-simulada-teste-guarantee',
      maxInFlightRequests: 1,
      idempotent: true
    });
    await producer.connect();
    await producer.send({
        topic: 'teste-kafka',
        acks: -1,
        timeout: 60000,
        compression: CompressionTypes.GZIP,
        messages: mensagens,
    });
    await producer.disconnect();
}

Publicar(['Hello KafkaJS user!'])