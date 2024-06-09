import React from 'react';
import Card from 'react-bootstrap/Card';
import CardTitle from 'react-bootstrap/CardTitle'


const cardVendas = (valor) => (
  <Card className="text-center qtd-vendas">
    <Card.Body className='card-body-back-color'>
      <CardTitle as='div' className='card-vendas-lable'>VENDAS</CardTitle>
      <Card.Text className='card-vendas-value'>#{valor}</Card.Text>
    </Card.Body>
  </Card>
)

const  QtdVendas = ({valor}) => cardVendas(valor)

export default QtdVendas;
