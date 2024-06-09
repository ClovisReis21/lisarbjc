import React from 'react';
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  BarElement,
  Title,
  Tooltip,
  Legend,
} from 'chart.js';
import { Bar } from 'react-chartjs-2';
import ChartDataLabels from 'chartjs-plugin-datalabels';

ChartJS.register(
  CategoryScale,
  LinearScale,
  BarElement,
  Title,
  Tooltip,
  Legend,
  ChartDataLabels,
);

const options = {
  responsive: true,
  plugins: {
    legend: {
      position: 'top',
    },
    title: {
      display: true,
      text: 'PRODUTOS MAIS VENDIDOS',
      font: {size: 20}
    },
    datalabels: {
      anchor: 'end',
      clamp: true
    }
  },
};

const  MaisVendidos = ({data}) => <Bar options={options} data={data} />

export default MaisVendidos;
