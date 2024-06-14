const express = require("express");
const http = require("http");
const { Server } = require("socket.io");
const promClient = require('prom-client');
const kafka = require('./kafkajs');

const api_dashboard_porta = process.env.API_DASHBOARD_PORTA;
const app = express();
const server = http.createServer(app);
const metricsInterval = promClient.collectDefaultMetrics();
const totalRequests = new promClient.Counter({
  name: 'total_requests',
  help: 'Total de requisições recebidas pelo servidor',
  labelNames: ['ip'] // Adicionando um rótulo para o endereço IP
});

app.use(express.json());
app.use((req, res, next) => {
  const ip = req.headers['x-forwarded-for'] || req.connection.remoteAddress;
  totalRequests.inc({ ip });
  next();
});

app.get('/metrics', async (req, res) => {
  res.set('Content-Type', promClient.register.contentType);
  res.end(await promClient.register.metrics());
});

async function delay(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms))
}

const io = new Server(server, {
  cors: {
    origin: "*",
  },
});

module.exports = wsServer = async () => {
  console.log('dotenvs:',
    process.env.API_DASHBOARD_PORTA, '-',
    process.env.KAFKA_BROKERCONNECT, '-',
    process.env.KAFKA_CLIENTE_ID, '-',
    process.env.KAFKA_GRUPO, '-',
    process.env.KAFKA_TOPICOS,
  )
  await delay(10000)
  io.on("connection", (socket) => {
    console.log("New client is connected");

    socket.on("disconnect", () => {
      console.log("Client disconnected");
    });
  });
  const atualizarDashboard = async (dados) => {
      io.emit('message', dados);
      console.log(dados, 'envidados');
  }
  kafka.consumer(atualizarDashboard);
  try {
    server.listen(api_dashboard_porta, () => {
      console.log(`Connected successfully on port ${api_dashboard_porta}`);
    });
  } catch (error) {
    console.error(`Error occurred: ${error.message}`);
  }
}