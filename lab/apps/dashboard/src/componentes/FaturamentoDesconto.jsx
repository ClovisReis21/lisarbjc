import React from 'react';
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  BarElement,
  Title,
  Tooltip,
  Legend,
  ArcElement,
} from 'chart.js';
import { Pie } from 'react-chartjs-2';
import ChartDataLabels from 'chartjs-plugin-datalabels';

ChartJS.register(
  CategoryScale,
  LinearScale,
  BarElement,
  ArcElement,
  Title,
  Tooltip,
  Legend,
  ChartDataLabels,
);

const  FaturamentoDesconto = ({data}) => {
  const options = {
    responsive: true,
    plugins: {
      legend: false,
      title: {
        display: true,
        text: 'TOTAL R$ ' + data.datasets[0].faturamentoTotal,
        font: {size: 25},
      },
      datalabels: {
        anchor: 'end',
        clamp: true,
        color: 'white',
        display: true,
        font: {
          weight: 'bold',
        },
        offset: 0,
        padding: 0
      },
    },
    // Core options
    aspectRatio: 3 / 2,
    layout: {
      padding: 20
    },
  };
  return (
    <Pie options={options} data={data} />
  )
}

export default FaturamentoDesconto;
