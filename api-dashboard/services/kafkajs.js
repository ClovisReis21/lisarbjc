const { Kafka } = require('kafkajs');
const { atualizarDashboard } = require ('../app');

const kafka = new Kafka({
  clientId: 'dashboards-consumer',
  brokers: ['localhost:9092'],
});

const group = 'dashboards-consumer';

async function primeiraLeitura(data) {
  return await JSON.parse(data)
}

async function consumer(fn_atualiza_dashboard) {
  const consumer = kafka.consumer({ groupId: group });
  await consumer.connect();

  await consumer.subscribe({ topics: ["vendas-deshboard-gold"], fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ topic, partition, message, heartbeat, pause }) => {
      let dados;
      try{
        dados = await primeiraLeitura(message.value.toString())
      } catch(e) {
        console.log(`ERRO --> `, e, message.value.toString());
        return;
      }
      if (!dados || !dados.grafico) return;
      switch (dados.grafico) {
        case 'pizza':
          fn_atualiza_dashboard(JSON.stringify({
            grafico: dados.grafico,
            valorFaturamento: dados.ideal,
            labels: ['VENDA','DESCONTO'],
            valores: [dados.venda, dados.desconto],
            percentuais: [ dados.percentual_venda, dados.percentual_desconto ],
          }))
          break;
        default:
          break;
      }
      return;
    },
  });
}

module.exports = { consumer }