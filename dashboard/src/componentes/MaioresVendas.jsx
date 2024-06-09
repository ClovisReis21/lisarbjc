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
  indexAxis: 'y',
  responsive: true,
  padding: 50,
  plugins: {
    legend: false
    // {
    //   position: 'top',
    // }
    ,
    title: {
      display: true,
      text: 'MAIORES VENDAS vs VENDEDOR',
      font: {size: 20}
    },
    datalabels: {
      anchor: 'end',
      clamp: true
    }
  },
};

const  MaioresVendas = ({data}) => <Bar options={options} data={data} />

export default MaioresVendas;
