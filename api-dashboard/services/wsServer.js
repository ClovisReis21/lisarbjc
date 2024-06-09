const express = require("express");
const http = require("http");
const { Server } = require("socket.io");

const kafka = require('./kafkajs')

const PORT = 4000;

// Create an express applicaiton
const app = express();
app.use(express.json());

// Create a Socket.io server with the express application
const server = http.createServer(app);
const io = new Server(server, {
    cors: {
      origin: "*",
    },
  });

  module.exports = wsServer = () => {
  // wait for new connection
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

  // Run the server
  try {
    server.listen(PORT, () => {
      console.log(`Connected successfully on port ${PORT}`);
    });
  } catch (error) {
    console.error(`Error occurred: ${error.message}`);
  }
}