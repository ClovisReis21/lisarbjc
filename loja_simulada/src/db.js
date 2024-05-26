const mysql = require("mysql2/promise");

const dataConn = {
    host: process.env.LOCAL_DB_HOST,
    user: process.env.LOCAL_DB_USER,
    password: process.env.LOCAL_DB_PASS,
    database: process.env.LOCAL_DB_DATABASE,
}

async function conectar () {
    if(global.con && global.con.state !== 'disconnected')
        return global.con;
    
    const con = await mysql.createConnection(dataConn)
    console.log("conectado ao MySQL!");//DEBUG
    global.con = con;
    return con;
}

async function desconectar () {
    if(global.con && global.con.state !== 'disconnected') {
        await con.end()
        console.log("Desconectarado do MySQL!")//DEBUG
        global.con = con;
        return con;
    }
}

async function insereCliente(cliente) {
    const conn = await conectar();
    const [existe] = await conn.query(`SELECT cpf FROM clientes WHERE cpf = '${cliente.cpf}'`)
    if (existe.length > 0) return undefined;
    const [response] = await conn.query(`
        INSERT INTO
        clientes (cpf, telefone, email, cliente, estado, origem_racial, sexo, status)
        VALUES(
            '${cliente.cpf}','${cliente.telefone}','${cliente.email}','${cliente.cliente}','${cliente.estado}','${cliente.origem_racial}','${cliente.sexo}','${cliente.status}')
    `);
    return response.insertId
}

async function insereProduto(produto) {
    const conn = await conectar();
    const [existe] = await conn.query(`SELECT produto FROM produtos WHERE produto = '${produto.produto}'`)
    if (existe.length > 0) return undefined;
    const [response] = await conn.query(`
        INSERT INTO produtos (produto, preco)
        VALUES ('${produto.produto}', ${produto.preco})
    `);
    return response.insertId
}

async function insereVendedor(vendedor) {
    const conn = await conectar();
    const [existe] = await conn.query(`SELECT cpf FROM vendedores WHERE cpf = '${vendedor.cpf}'`)
    if (existe.length > 0) return undefined;
    const [response] = await conn.query(`
        INSERT INTO vendedores (cpf, telefone, email, origem_racial, nome)
        VALUES ('${vendedor.cpf}','${vendedor.telefone}','${vendedor.email}','${vendedor.origem_racial}','${vendedor.nome}');
    `);
    return response.insertId
}

async function insereVenda(venda) {
    const conn = await conectar();
    const [response] = await conn.query(`
        INSERT INTO vendas (id_vendedor, id_cliente, data, total)
        VALUES (${venda.id_vendedor}, ${venda.id_cliente},'${venda.data}', ${venda.total})
    `);
    return await response.insertId
}

async function insereItemVenda(itemVenda) {
    const conn = await conectar();
    const [response] = await conn.query(`
        INSERT INTO itens_venda (id_produto, id_venda, quantidade, valor_unitario, valor_total, desconto)
        VALUES (${itemVenda.id_produto}, ${itemVenda.id_venda},${itemVenda.quantidade}, ${itemVenda.valor_unitario},${itemVenda.valor_total}, ${itemVenda.desconto})
    `);
    return await response.insertId
}

async function getIdClientes() {
    const conn = await conectar();
    const [response] = await conn.query(`SELECT id_cliente FROM clientes`);
    const result = []
    response.forEach(element =>  result.push(Object.values(element)[0]));
    return result;
}

async function getProdutos() {
    const conn = await conectar();
    const [response] = await conn.query(`SELECT id_produto, preco FROM produtos`);
    return response;
}

async function getIdVendedores() {
    const conn = await conectar();
    const [response] = await conn.query(`SELECT id_vendedor FROM vendedores`);
    const result = []
    response.forEach(element =>  result.push(Object.values(element)[0]));
    return result;
}

const newTuplas = {
    cliente: [
        { cpf: '051.008.262-15', telefone: '(21) 93041-7232', email: 'inacio.sobrinho@email.com', cliente: 'Inacio Túlio Aranda Sobrinho',                estado: 'SP',   origem_racial: 'preta',   sexo: 'M',   status: 'Silver' },
        { cpf: '277.548.397-33', telefone: '(24) 96464-6217', email: 'camila.abreu@email.com',    cliente: 'Camila Ivanete Abreu',                        estado: 'GO',   origem_racial: 'parda',   sexo: 'F',   status: 'Silver' },
        { cpf: '940.798.269-21', telefone: '(24) 99157-3262', email: 'david.rocha@email.com',     cliente: 'David Ferminiano Rocha',                      estado: 'MT',   origem_racial: 'branca',  sexo: 'M',   status: 'Silver' },
        { cpf: '740.028.417-45', telefone: '(33) 94735-7078', email: 'eliomar.fontes@email.com',  cliente: 'Eliomar Batista Fontes',                      estado: 'CE',   origem_racial: 'branca',  sexo: 'M',   status: 'Silver' },
        { cpf: '690.408.517-75', telefone: '(47) 95256-4833', email: 'gian.serrano@email.com',    cliente: 'Gian Augusto Ferreira de Serrano',            estado: 'SP',   origem_racial: 'branca',  sexo: 'M',   status: 'Platinum' },
        { cpf: '990.978.743-62', telefone: '(74) 98231-9390', email: 'josiane.gomes@email.com',   cliente: 'Josiane Paula Colaço de Gomes',               estado: 'SP',   origem_racial: 'parda',   sexo: 'F',   status: 'Silver' },
        { cpf: '190.788.045-60', telefone: '(83) 94242-4934', email: 'hugo.jr@email.com',         cliente: 'Hugo Santiago Fidalgo Jr.',                   estado: 'MS',   origem_racial: 'preta',   sexo: 'M',   status: 'Silver' },
        { cpf: '940.328.341-00', telefone: '(37) 98259-4069', email: 'luzia.salgado@email.com',   cliente: 'Luzia Zulmira Aguiar de Salgado',             estado: 'MG',   origem_racial: 'parda',   sexo: 'F',   status: 'Silver' },
        { cpf: '330.458.391-71', telefone: '(28) 91012-1788', email: 'adriana.lira@email.com',    cliente: 'Adriana Laila Ferreira Lira',                 estado: 'PE',   origem_racial: 'parda',   sexo: 'F',   status: 'Gold' },
        { cpf: '770.298.139-98', telefone: '(65) 94504-0814', email: 'laerte.linhares@email.com', cliente: 'Laerte Nicolas Azevedo Dominato de Linhares', estado: 'AM',   origem_racial: 'amarela', sexo: 'M',   status: 'Silver' }
    ],
    produto: [
        { produto: 'Ólulos de proteção', preco: 120.00 },
        { produto: 'Suporte de teto para carro', preco: 300.00 },
    ],
    vendedor: [
        { cpf: '902.331.481-67', telefone: '(37) 99504-7180', email: 'kevin_santiago@lojasimulada.com.br',   origem_racial: 'preta',    nome: 'Kevin Delatorre de Santiago'          },
        { cpf: '151.233.018-36', telefone: '(42) 91095-6284', email: 'oliver_jr@lojasimulada.com.br',        origem_racial: 'branca',   nome: 'Oliver Renato Corona Jr.'             },
        { cpf: '267.260.068-61', telefone: '(82) 97538-4220', email: 'silvana_caldeira@lojasimulada.com.br', origem_racial: 'branca',   nome: 'Silvana Bittencourt de Caldeira'      },
        { cpf: '422.539.945-10', telefone: '(38) 97154-3186', email: 'aline_cordeiro@lojasimulada.com.br',   origem_racial: 'preta',    nome: 'Aline Gabrielle de Cordeiro'          },
        { cpf: '077.017.268-70', telefone: '(19) 93953-0874', email: 'gustavo_souza@lojasimulada.com.br',    origem_racial: 'indígena', nome: 'Gustavo Lúcio Godói Padilha de Souza' },
        { cpf: '021.867.974-29', telefone: '(83) 92259-1277', email: 'hugo_sobrinho@lojasimulada.com.br',    origem_racial: 'parda',    nome: 'Hugo Feliciano Sobrinho'              },
        { cpf: '947.233.477-63', telefone: '(28) 92916-1765', email: 'andre_benites@lojasimulada.com.br',    origem_racial: 'branca',   nome: 'André Benites'                        },
        { cpf: '952.520.765-02', telefone: '(62) 90557-3533', email: 'altino_jimenes@lojasimulada.com.br',   origem_racial: 'indígena', nome: 'Altino Jânio Duarte de Jimenes'       },
        { cpf: '016.425.548-63', telefone: '(93) 91715-6737', email: 'amarildo_pedrosa@lojasimulada.com.br', origem_racial: 'parda',    nome: 'Amarildo Baltazar Mendes de Pedrosa'  },
        { cpf: '838.050.524-72', telefone: '(71) 93044-8405', email: 'simon_meireles@lojasimulada.com.br',   origem_racial: 'preta',    nome: 'Simon Silvair Godói de Meireles'      },
    ],
}

const getNewTuplas = (entidade) => {
    return newTuplas[entidade]
}

module.exports = {
    insereCliente,
    insereProduto,
    insereVendedor,
    insereVenda,
    insereItemVenda,
    getNewTuplas,
    getIdClientes,
    getProdutos,
    getIdVendedores,
}