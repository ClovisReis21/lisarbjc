import React, { useState, useEffect } from 'react';
import io from "socket.io-client";
import 'bootstrap/dist/css/bootstrap.min.css';
import './App.css';

import Container from 'react-bootstrap/Container';
import FaturamentoDesconto from './componentes/FaturamentoDesconto.jsx';

const react_app_api_dashboard = process.env.REACT_APP_API_DASHBOARD

const socket = io(react_app_api_dashboard);

function abreviarSifra(valor) {
  const valores = valor?.toString().split(/\./);
  let strValor, response;
  if (valores?.length > 0)
      strValor = valores[0]
  if (strValor?.length < 4) return strValor;
  const monet = ['K', 'M', 'MM', 'MM+'];
  for (let i = 3, m = 0; i < strValor?.length; i = i + 3, m++) {
      response = `${strValor.slice((-3-i), -i)} ${monet[m > 3 ? 3 : m]}`
  }
  return response
}

function App() {
  console.log('react_app_api_dashboard', process.env)
  const [faturamentoDesconto, setFaturamentoDesconto] = useState({
    valorFaturamento: 0,
    labels: ['VENDA','DESCONTO'],
    valores: [ 0, 0 ],
    percentuais: [ 0, 0 ],
  });

  useEffect(() => {
    socket.on("message", async (message) => {
      console.log('mensagem rcebida:', message)
      atualizaGrafico(JSON.parse(message))
    });
  }, []);

  const atualizaGrafico = (dados) => {
    console.log('dados enviados:', dados)
    if (!dados || !dados.grafico || dados.grafico !== 'pizza') return;
    console.log('dados enviados:', dados)
    setFaturamentoDesconto(dados);
    return;
  }

  const dataFaturamentoDesconto = {
    labels: faturamentoDesconto.labels,
    datasets: [{
      faturamentoTotal: abreviarSifra(faturamentoDesconto.valorFaturamento),
      data: [faturamentoDesconto.valores[0], faturamentoDesconto.valores[1]],
      backgroundColor: ['#A3E0BB', '#FFBD27'],
      datalabels: {
        labels: {
          name: {
            anchor: true,
            align: 'top',
            color: 'gray',
            font: {size: 16},
            formatter: (idx) => faturamentoDesconto.labels[faturamentoDesconto.valores.indexOf(idx)],
          },
          index: {
            anchor: true,
            align: 'top',
            color: 'blue',
            font: {size: 20},
            formatter: (idx) => {
              return faturamentoDesconto.percentuais[faturamentoDesconto.valores.indexOf(idx)].toFixed(0) + ' %'
            },
            offset: 20,
          },
          value: {
            anchor: true,
            align: 'bottom',
            backgroundColor: 'white',
            borderColor: 'white',
            borderWidth: 2,
            borderRadius: 4,
            color: 'blue',
            formatter:  (idx) =>  abreviarSifra(faturamentoDesconto.valores[faturamentoDesconto.valores.indexOf(idx)]),
            font: {size: 15},
            padding: 2
          }
        }
      }
    }]
  };

  return (
    <Container fluid className='container'>
      <FaturamentoDesconto data={dataFaturamentoDesconto} />
    </Container>
  );
}

export default App;


