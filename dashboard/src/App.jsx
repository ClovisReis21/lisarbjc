import React, { useState, useEffect } from 'react';
import './App.css';

import 'bootstrap/dist/css/bootstrap.min.css';
import Container from 'react-bootstrap/Container';
import Row from 'react-bootstrap/Row';
import Col from 'react-bootstrap/Col';

import MaisVendidos from './componentes/MaisVendidos.jsx';
import QtdVendas from './componentes/QtdVendas.jsx';
import MaioresVendas from './componentes/MaioresVendas.jsx';
import FaturamentoDesconto from './componentes/FaturamentoDesconto.jsx';

import io from "socket.io-client";

const socket = io("http://localhost:4000");

///////////////////////////////////////////////////////////////////////////////////////////////////

var triggerInterval = null
let changes = 10


function UpdateChangeState() {
  const [, setValue] = useState(0);
  return () => setValue(value => value + 1)
}

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

function formatarSifra(valor) {
    if (isNaN(valor)) return valor;
    let strValor = Number(valor).toString()
    if (strValor.search(/\./g) < 0) strValor = strValor + '.00';
    strValor = strValor.replace(/\./g,',')
    if (strValor.length < 7) return strValor.replace(/([0-9]{0,3})(\,[0-9]{2})/g,'$1$2');
    if (strValor.length < 10) return strValor.replace(/([0-9]{0,3})([0-9]{3})(\,[0-9]{2})/g,'$1.$2$3');
    if (strValor.length < 13) return strValor.replace(/([0-9]{0,3})([0-9]{3})([0-9]{3})(\,[0-9]{2})/g,'$1.$2.$3$4');
    if (strValor.length < 16) return strValor.replace(/([0-9]{0,3})([0-9]{3})([0-9]{3})([0-9]{3})(\,[0-9]{2})/g,'$1.$2.$3.$4$5');
    if (strValor.length < 19) return strValor.replace(/([0-9]{0,3})([0-9]{3})([0-9]{3})([0-9]{3})([0-9]{3})(\,[0-9]{2})/g,'$1.$2.$3.$4.$5$6');
    if (strValor.length < 22) return strValor.replace(/([0-9]{0,3})([0-9]{3})([0-9]{3})([0-9]{3})([0-9]{3})([0-9]{3})(\,[0-9]{2})/g,'$1.$2.$3.$4.$5.$6$7');
    return strValor
}

function App() {
  const [vendas, setVendas] = useState({qtdVenda: 0})
  const [faturamentoDesconto, setFaturamentoDesconto] = useState({
    valorFaturamento: 0,
    labels: ['VENDA','DESCONTO'],
    valores: [ 0, 0 ],
    percentuais: [ 0, 0 ],
  });
  const [maioresVendas, setMaioresVendas] = useState({
    valores: [0, 0, 0, 0, 0],
    labels: ['', '', '', '', '']
  });
  const [maisVendidos, setMaisVendidos] = useState({
    dados: [
      { label: 'PROD', data: [0, 0, 0, 0], backgroundColor:  'lime'   },
      { label: 'PROD', data: [0, 0, 0, 0], backgroundColor:  'skyblue'  },
      { label: 'PROD', data: [0, 0, 0, 0], backgroundColor:  'silver'   },
    ],
    labels: ['QTD', 'VALOR VENDA (R$)', 'DESCONTO (R$)', 'FATURAMENTO (R$)']
  });

  useEffect(() => {
    socket.on("message", async (message) => {
      // console.log("received a new message from the server", JSON.parse(message));
      atualizaGrafico(JSON.parse(message))
    });
  }, []);

  const atualizaGrafico = (dados) => {
    if (!dados || !dados.grafico) return;
    switch (dados.grafico) {
      case 'pizza':
        setFaturamentoDesconto(dados);
        break;    
      default:
        break;
    }
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

  const dataMaioresVendas = {
    labels: maioresVendas.labels,
    datasets: [
      {
        data: maioresVendas.valores,
        borderColor: 'red',
        backgroundColor: ['#32CD32', '#00FF7F', '#98FB98', '#FFE4B5', '#FFA07A'],
        datalabels: {
          labels: {
            value: {
              color: 'bleck',
              anchor: false,
              align: 'center',
              // color: 'gray',
              font: {size: 15},
              formatter: (idx) => 'R$ ' + formatarSifra(maioresVendas.valores[maioresVendas.valores.indexOf(idx)]),
            }
          }
        }
      }
    ]
  };

  const dataMaisVendidos = {
    labels: maisVendidos.labels,
    datasets: [
      {
        label: maisVendidos.dados[0].label,
        data: maisVendidos.dados[0].data,
        backgroundColor:maisVendidos.dados[0].backgroundColor,
        datalabels: {
          labels: {
            value: {
              color: 'bleck',
              anchor: false,
              align: 'center',
              font: {size: 15},
              formatter: (idx) => abreviarSifra(maisVendidos.dados[0].data[maisVendidos.dados[0].data.indexOf(idx)]),
            }
          }
        }
      },
      {
        label: maisVendidos.dados[1].label,
        data: maisVendidos.dados[1].data,
        backgroundColor:maisVendidos.dados[1].backgroundColor,
        datalabels: {
          labels: {
            value: {
              color: 'bleck',
              anchor: false,
              align: 'center',
              font: {size: 15},
              formatter: (idx) => abreviarSifra(maisVendidos.dados[0].data[maisVendidos.dados[0].data.indexOf(idx)]),
            }
          }
        }
      },
      {
        label: maisVendidos.dados[2].label,
        data: maisVendidos.dados[2].data,
        backgroundColor:maisVendidos.dados[2].backgroundColor,
        datalabels: {
          labels: {
            value: {
              color: 'bleck',
              anchor: false,
              align: 'center',
              font: {size: 15},
              formatter: (idx) => abreviarSifra(maisVendidos.dados[0].data[maisVendidos.dados[0].data.indexOf(idx)]),
            }
          }
        }
      },
    ],
  };

  const forceUpdate = UpdateChangeState();
  // if (!triggerInterval) {
  //     triggerInterval = setInterval(() => {
  //       changes = changes + 1;
  //       qtdVenda = qtdVenda + 1;
  //       data = labelsFaturamentoDesconto.map(() => Math.floor(Math.random() * 1000) + 1)
  //       forceUpdate()
  //     }, 3000);
  // }

  return (
    <Container fluid className='container'>
      <FaturamentoDesconto data={dataFaturamentoDesconto} />
      {/* <Row>
        <Col>
            <QtdVendas valor={vendas.qtdVenda} />
        </Col>
        <Col>
            <FaturamentoDesconto data={dataFaturamentoDesconto} />
        </Col>
      </Row>
      <Row>
        <Col>
            <MaioresVendas data={dataMaioresVendas} />
        </Col>
        <Col>
            <MaisVendidos data={dataMaisVendidos} />
        </Col>
      </Row> */}
    </Container>
  );
}

export default App;


