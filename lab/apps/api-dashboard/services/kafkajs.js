const { Kafka } = require('kafkajs');

const dataConn = {
  kafka_brokerconnect: process.env.KAFKA_BROKERCONNECT,
  kafka_cliente_id: process.env.KAFKA_CLIENTE_ID,
  kafka_grupo: process.env.KAFKA_GRUPO,
  kafka_topicos: process.env.KAFKA_TOPICOS,
};


const kafka = new Kafka({
  clientId: dataConn.kafka_cliente_id,
  brokers: [dataConn.kafka_brokerconnect],
});

const group = dataConn.kafka_grupo;

async function primeiraLeitura(data) {
  return await JSON.parse(data);
}

async function consumer(fn_atualiza_dashboard) {
  try {    
    const consumer = kafka.consumer({ groupId: group });
    await consumer.connect();
    await consumer.subscribe({ topics: [dataConn.kafka_topicos], fromBeginning: false });
    await consumer.run({
      eachMessage: async ({ message }) => {
        let dados;
        try{
          dados = await primeiraLeitura(message.value.toString());
        } catch(e) {
          console.log(`ERRO --> `, e, message.value.toString());
          return;
        }
        if (!dados || !dados.grafico || dados.grafico != 'pizza') return;
        fn_atualiza_dashboard(JSON.stringify({
          grafico: dados.grafico,
          valorFaturamento: dados.ideal,
          labels: ['VENDA','DESCONTO'],
          valores: [dados.venda, dados.desconto],
          percentuais: [ dados.percentual_venda, dados.percentual_desconto ],
        }));
        return;
      },
    });
    return;
  } catch (error) {
    console.log('erro ao conectar ao kafka => Alerta de erro!');
    consumer(fn_atualiza_dashboard);
  }
}

module.exports = { consumer }