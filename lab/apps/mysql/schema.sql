CREATE DATABASE IF NOT EXISTS vendas;
CREATE USER IF NOT EXISTS 'big_data_importer'@'%' IDENTIFIED BY 'big_data_importer';
GRANT SELECT ON vendas.* TO 'big_data_importer'@'%';
FLUSH PRIVILEGES;

USE vendas;

CREATE TABLE IF NOT EXISTS vendedores (
  id_vendedor INT NOT NULL AUTO_INCREMENT,
  cpf VARCHAR(14),
  telefone VARCHAR(15),
  email VARCHAR(60),
  origem_racial VARCHAR(10),
  nome VARCHAR(50),
  PRIMARY KEY (id_vendedor)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS produtos (
  id_produto INT NOT NULL AUTO_INCREMENT,
  produto VARCHAR(100),
  preco DECIMAL(10,2),
  PRIMARY KEY (id_produto)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS clientes (
  id_cliente INT NOT NULL AUTO_INCREMENT,
  cpf VARCHAR(14),
  telefone VARCHAR(15),
  email VARCHAR(60),
  cliente VARCHAR(50),
  estado VARCHAR(2),
  origem_racial VARCHAR(10),
  sexo CHAR(1),
  status VARCHAR(50),
  PRIMARY KEY (id_cliente)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS vendas (
  id_venda INT NOT NULL AUTO_INCREMENT,
  id_vendedor INT,
  id_cliente INT,
  data DATE,
  total DECIMAL(10,2),
  PRIMARY KEY (id_venda),
  FOREIGN KEY (id_vendedor) REFERENCES vendedores(id_vendedor),
  FOREIGN KEY (id_cliente) REFERENCES clientes(id_cliente)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS itens_venda (
  id_produto INT NOT NULL,
  id_venda INT NOT NULL,
  quantidade INT,
  valor_unitario DECIMAL(10,2),
  valor_total DECIMAL(10,2),
  desconto DECIMAL(10,2),
  PRIMARY KEY (id_produto, id_venda),
  FOREIGN KEY (id_produto) REFERENCES produtos(id_produto) ON DELETE RESTRICT,
  FOREIGN KEY (id_venda) REFERENCES vendas(id_venda) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
