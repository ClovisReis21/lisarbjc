const mysql = require("mysql2/promise");

const dataConn = {
    host: process.env.DB_HOST,
    user: process.env.DB_USER,
    password: process.env.DB_PASS,
    database: process.env.DB_NAME,
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
        const con = await global.con.resume()
        console.log("Desconectado do MySQL!")//DEBUG
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

async function delay(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms))
}

async function migrations() {
    await delay(1000)
    try {
        const conn = await conectar();
        const [clientes] = await conn.query(`SELECT id_cliente FROM clientes limit 1`)
        console.log('migrations() => clientes OK');
        if (clientes.length < 1) {
            await conn.query(`
                INSERT INTO clientes (cpf, telefone, email, cliente, estado, origem_racial, sexo, status)
                VALUES 
                ('802.404.251-70', '(82) 96797-3891', 'adelina.buenaventura@email.com',   'Adelina Buenaventura',            'RJ', 'preta'      , 'M', 'Silver')
                ,('245.547.921-88', '(69) 93170-3038', 'adelino.gago@email.com',           'Adelino Gago',                    'RJ', 'branca'     , 'M', 'Silver')
                ,('312.106.323-57', '(55) 93138-0574', 'adelio.lisboa@email.com',          'Adélio Lisboa',                   'SE', 'branca'     , 'M', 'Silver')
                ,('176.174.643-04', '(95) 96122-1491', 'aderito.bahia@email.com',          'Adérito Bahía',                   'MA', 'preta'      , 'M', 'Silver')
                ,('327.670.313-01', '(44) 91828-7201', 'adolfo.patricio@email.com',        'Adolfo Patrício',                 'PE', 'indígena'   , 'M', 'Silver')
                ,('055.321.654-67', '(49) 97648-5448', 'adriana.guedelha@email.com',       'Adriana Guedelha',                'RO', 'parda'      , 'F', 'Platinum')
                ,('942.199.479-57', '(53) 98450-1263', 'aida.dorneles@email.com',          'Aida Dorneles',                   'RN', 'branca'     , 'F', 'Silver')
                ,('249.222.081-86', '(96) 96055-1698', 'alarico.quinterno@email.com',      'Alarico Quinterno',               'AC', 'indígena'   , 'M', 'Silver')
                ,('024.056.277-10', '(24) 90537-9980', 'alberto.cezimbra@email.com',       'Alberto Cezimbra',                'AM', 'parda'      , 'M', 'Silver')
                ,('342.332.856-80', '(83) 99537-8396', 'alberto.monsanto@email.com',       'Alberto Monsanto',                'RN', 'preta'      , 'M', 'Gold'  )
                ,('800.612.060-91', '(54) 95703-2870', 'albino.canela@email.com',          'Albino Canela',                   'AC', 'preta'      , 'M', 'Silver')
                ,('372.883.136-03', '(33) 93960-3271', 'alceste.varanda@email.com',        'Alceste Varanda',                 'RR', 'parda'      , 'F', 'Silver')
                ,('754.552.048-31', '(79) 94882-4684', 'alcides.carvalhais@email.com',     'Alcides Carvalhais',              'RO', 'branca'     , 'M', 'Silver')
                ,('276.698.078-49', '(68) 92498-5308', 'aldo.martins@email.com',           'Aldo Martins',                    'GO', 'branca'     , 'M', 'Silver')
                ,('454.783.006-16', '(97) 96932-7096', 'alexandra.tabares@email.com',      'Alexandra Tabares',               'MG', 'branca'     , 'F', 'Silver')
                ,('397.227.336-30', '(49) 95697-9236', 'alfredo.cotrim@email.com',         'Alfredo Cotrim',                  'SC', 'parda'      , 'M', 'Silver')
                ,('961.053.777-46', '(24) 92271-6431', 'almeno.figueira@email.com',        'Almeno Figueira',                 'SC', 'preta'      , 'M', 'Silver')
                ,('232.000.152-25', '(27) 93098-5244', 'alvito.peralta@email.com',         'Alvito Peralta',                  'AM', 'parda'      , 'M', 'Silver')
                ,('661.296.614-95', '(27) 97171-6704', 'amadeu.martinho@email.com',        'Amadeu Martinho',                 'RN', 'parda'      , 'M', 'Silver')
                ,('978.568.136-01', '(33) 99306-0243', 'amelia.estevez@email.com',         'Amélia Estévez',                  'PE', 'amarela'    , 'F', 'Silver')
                ,('349.224.033-02', '(38) 92590-6803', 'ana.homem@email.com',              'Ana Homem',                       'RN', 'parda'      , 'F', 'Silver')
                ,('497.564.681-14', '(46) 99625-0879', 'ana.peseiro@email.com',            'Ana Peseiro',                     'PA', 'indígena'   , 'F', 'Silver')
                ,('010.299.746-24', '(98) 90635-4785', 'anacleto.garrau@email.com',        'Anacleto Garrau',                 'TO', 'parda'      , 'M', 'Silver')
                ,('548.543.528-92', '(93) 90354-7366', 'anacleto.guterres@email.com',      'Anacleto Guterres',               'PI', 'parda'      , 'M', 'Silver')
                ,('291.730.022-16', '(83) 94051-5839', 'andreia.carvalhal@email.com',      'Andreia Carvalhal',               'SP', 'parda'      , 'F', 'Silver')
                ,('601.730.167-58', '(81) 90096-5219', 'anibal.bastos@email.com',          'Aníbal Bastos',                   'CE', 'branca'     , 'M', 'Silver')
                ,('077.269.135-50', '(41) 92465-5361', 'anita.beserra@email.com',          'Anita Beserra',                   'PE', 'indígena'   , 'F', 'Silver')
                ,('763.502.504-00', '(31) 93400-7138', 'anna.beca@email.com',              'Anna Beça',                       'AP', 'indígena'   , 'F', 'Silver')
                ,('061.562.710-23', '(66) 91112-4930', 'anna.carvajal@email.com',          'Anna Carvajal',                   'RS', 'parda'      , 'F', 'Gold'  )
                ,('985.370.031-07', '(49) 92169-3559', 'anselmo.dinis@email.com',          'Anselmo Dinis',                   'PA', 'indígena'   , 'M', 'Silver')
                ,('750.917.930-03', '(96) 91380-7508', 'antao.corte@email.com',            'Antão Corte-Real',                'ES', 'parda'      , 'M', 'Silver')
                ,('996.711.124-04', '(87) 99896-0626', 'antero.milheiro@email.com',        'Antero Milheiro',                 'AP', 'preta'      , 'M', 'Silver')
                ,('096.161.403-02', '(71) 96033-3200', 'antonia.canhao@email.com',         'Antónia Canhão',                  'SC', 'preta'      , 'F', 'Silver')
                ,('335.713.479-94', '(31) 93549-7916', 'antonio.leiria@email.com',         'Antônio Leiria',                  'AL', 'parda'      , 'M', 'Silver')
                ,('374.049.304-64', '(41) 99540-0772', 'antonio.lousado@email.com',        'António Lousado',                 'RS', 'indígena'   , 'M', 'Silver')
                ,('041.404.320-04', '(97) 94346-5764', 'antonio.sobral@email.com',         'Antônio Sobral',                  'AC', 'amarela'    , 'M', 'Silver')
                ,('585.192.835-27', '(41) 96376-4632', 'apua.ourique@email.com',           'Apuã Ourique',                    'AL', 'indígena'   , 'M', 'Silver')
                ,('974.542.392-00', '(98) 99863-8086', 'arachane.matos@email.com',         'Arachane Matos',                  'MA', 'amarela'    , 'F', 'Silver')
                ,('438.924.707-74', '(34) 99398-1195', 'arcidres.murici@email.com',        'Arcidres Murici',                 'TO', 'amarela'    , 'M', 'Silver')
                ,('938.113.590-88', '(81) 96097-0392', 'armindo.castilhos@email.com',      'Armindo Castilhos',               'RR', 'preta'      , 'M', 'Silver')
                ,('147.209.391-71', '(16) 91476-7455', 'armindo.moreira@email.com',        'Armindo Moreira',                 'ES', 'branca'     , 'M', 'Silver')
                ,('837.299.906-67', '(69) 97688-9557', 'artur.macedo@email.com',           'Artur Macedo',                    'SE', 'amarela'    , 'M', 'Silver')
                ,('082.702.563-74', '(17) 90773-0547', 'artur.pena@email.com',             'Artur Peña',                      'RJ', 'amarela'    , 'M', 'Silver')
                ,('036.134.370-15', '(62) 95380-9873', 'aurelio.barrios@email.com',        'Aurélio Barrios',                 'PI', 'branca'     , 'M', 'Silver')
                ,('073.993.005-26', '(18) 92571-6702', 'barbara.magalhaes@email.com',      'Bárbara Magalhães',               'MG', 'preta'      , 'F', 'Silver')
                ,('702.621.071-80', '(37) 90449-6272', 'bartolomeu.vila@email.com',        'Bartolomeu Vila-Chã',             'TO', 'branca'     , 'M', 'Silver')
                ,('342.623.334-77', '(16) 96249-6114', 'basilio.soares@email.com',         'Basilio Soares',                  'AC', 'amarela'    , 'M', 'Silver')
                ,('618.971.414-59', '(95) 91249-8411', 'belmira.colaco@email.com',         'Belmira Colaço',                  'RJ', 'preta'      , 'F', 'Silver')
                ,('356.589.970-03', '(19) 98218-2227', 'belmiro.marroquim@email.com',      'Belmiro Marroquim',               'SC', 'amarela'    , 'M', 'Silver')
                ,('997.398.971-69', '(54) 91313-1408', 'bento.quintao@email.com',          'Bento Quintão',                   'SP', 'indígena'   , 'M', 'Gold'  )
                ,('246.544.483-22', '(65) 91536-1035', 'berengaria.iglesias@email.com',    'Berengária Iglesias',             'BA', 'parda'      , 'F', 'Silver')
                ,('273.585.022-64', '(18) 96542-0307', 'bernardete.agueda@email.com',      'Bernardete Águeda',               'TO', 'preta'      , 'F', 'Silver')
                ,('035.761.390-28', '(83) 97274-5331', 'bernardete.querino@email.com',     'Bernardete Querino',              'PI', 'branca'     , 'F', 'Silver')
                ,('081.476.510-66', '(91) 96655-4500', 'bernardete.tavera@email.com',      'Bernardete Tavera',               'ES', 'preta'      , 'F', 'Silver')
                ,('674.624.984-05', '(85) 95864-5232', 'braulio.junquera@email.com',       'Bráulio Junquera',                'PB', 'amarela'    , 'M', 'Silver')
                ,('750.423.180-03', '(48) 99166-6997', 'brenda.serralheiro@email.com',     'Brenda Serralheiro',              'PA', 'indígena'   , 'F', 'Silver')
                ,('539.012.755-25', '(41) 91905-1180', 'brigida.gusmao@email.com',         'Brígida Gusmão',                  'DF', 'branca'     , 'F', 'Silver')
                ,('491.721.287-12', '(92) 96442-9703', 'brites.morales@email.com',         'Brites Morales',                  'SC', 'indígena'   , 'F', 'Silver')
                ,('737.881.532-29', '(71) 99436-7050', 'bruno.perdigao@email.com',         'Bruno Perdigão',                  'MS', 'branca'     , 'M', 'Silver')
                ,('791.490.951-37', '(17) 97576-1092', 'bukake.nunez@email.com',           'Bukake Núñez',                    'SC', 'branca'     , 'F', 'Silver')
                ,('710.627.390-27', '(89) 94915-2881', 'caim.queiroga@email.com',          'Caím Queiroga',                   'PA', 'parda'      , 'M', 'Silver')
                ,('731.317.884-02', '(24) 92442-2039', 'calisto.britto@email.com',         'Calisto Britto',                  'RN', 'preta'      , 'M', 'Silver')
                ,('308.186.516-60', '(19) 92257-4625', 'candida.silvestre@email.com',      'Cândida Silvestre',               'MG', 'indígena'   , 'F', 'Silver')
                ,('171.625.246-64', '(98) 95452-9946', 'candido.sousa@email.com',          'Cândido Sousa doPrado',           'PR', 'amarela'    , 'M', 'Silver')
                ,('647.718.730-20', '(64) 96762-4293', 'capitolina.ruela@email.com',       'Capitolina Ruela',                'PI', 'branca'     , 'F', 'Silver')
                ,('668.831.991-87', '(91) 92265-6341', 'capitolina.valadares@email.com',   'Capitolina Valadares',            'GO', 'branca'     , 'F', 'Silver')
                ,('071.718.459-58', '(84) 94177-2757', 'carla.briones@email.com',          'Carla Briones',                   'AC', 'branca'     , 'F', 'Silver')
                ,('881.666.385-50', '(62) 95580-2310', 'carlos.murici@email.com',          'Carlos Murici',                   'MG', 'parda'      , 'M', 'Silver')
                ,('810.569.440-99', '(93) 92887-2783', 'carmem.reino@email.com',           'Carmem Reino',                    'CE', 'amarela'    , 'F', 'Silver')
                ,('579.590.987-43', '(54) 97043-0784', 'carminda.alcaide@email.com',       'Carminda Alcaide',                'RO', 'preta'      , 'F', 'Silver')
                ,('299.122.036-82', '(48) 90705-2548', 'carminda.dias@email.com',          'Carminda Dias',                   'AM', 'parda'      , 'F', 'Gold'  )
                ,('376.315.747-66', '(63) 97447-9500', 'cassia.guerra@email.com',          'Cássia Guerra',                   'MG', 'parda'      , 'F', 'Silver')
                ,('048.104.605-41', '(69) 90692-0368', 'cassia.queiroz@email.com',         'Cássia Queiroz',                  'AM', 'branca'     , 'F', 'Silver')
                ,('127.048.185-17', '(21) 94359-5936', 'cassio.bettencourt@email.com',     'Cássio Bettencourt',              'BA', 'indígena'   , 'M', 'Silver')
                ,('522.197.851-22', '(85) 97275-2366', 'catarina.bairros@email.com',       'Catarina Bairros',                'PA', 'preta'      , 'F', 'Silver')
                ,('243.389.579-05', '(89) 99454-9886', 'catarina.montero@email.com',       'Catarina Montero',                'PI', 'amarela'    , 'F', 'Silver')
                ,('592.868.508-40', '(93) 97967-6958', 'caue.cotrim@email.com',            'Cauê Cotrim',                     'PE', 'indígena'   , 'M', 'Silver')
                ,('799.550.776-38', '(99) 90868-6212', 'caue.parahyba@email.com',          'Cauê Parahyba',                   'PA', 'indígena'   , 'M', 'Silver')
                ,('832.844.968-40', '(93) 91273-8583', 'cecilia.carlos@email.com',         'Cecília Carlos',                  'AP', 'preta'      , 'F', 'Silver')
                ,('089.055.939-25', '(89) 99249-0856', 'celestino.pereira@email.com',      'Celestino Pereira',               'RS', 'parda'      , 'M', 'Silver')
                ,('377.486.371-72', '(73) 91384-6401', 'celia.meireles@email.com',         'Célia Meireles',                  'CE', 'preta'      , 'F', 'Silver')
                ,('661.621.777-93', '(81) 92375-4881', 'celina.vale@email.com',            'Celina Vale',                     'AL', 'branca'     , 'F', 'Silver')
                ,('926.789.872-80', '(86) 91699-4022', 'cesario.raminhos@email.com',       'Cesário Raminhos',                'RR', 'parda'      , 'M', 'Silver')
                ,('466.049.055-60', '(68) 94346-4877', 'cid.pardo@email.com',              'Cid Pardo',                       'AL', 'preta'      , 'M', 'Silver')
                ,('608.641.667-29', '(66) 92421-2573', 'cidalia.miera@email.com',          'Cidália Miera',                   'SE', 'indígena'   , 'F', 'Silver')
                ,('622.666.015-55', '(17) 94170-0781', 'cidalia.ornelas@email.com',        'Cidália Ornelas',                 'RS', 'indígena'   , 'F', 'Silver')
                ,('359.572.956-04', '(16) 92384-9065', 'claudio.cotegipe@email.com',       'Cláudio Cotegipe',                'MT', 'amarela'    , 'M', 'Silver')
                ,('883.329.875-20', '(27) 92897-7033', 'claudio.jorge@email.com',          'Cláudio Jorge',                   'TO', 'branca'     , 'M', 'Gold'  )
                ,('338.787.464-23', '(38) 98548-7429', 'claudio.lopes@email.com',          'Cláudio Lopes',                   'BA', 'indígena'   , 'M', 'Silver')
                ,('939.910.210-64', '(19) 90294-6494', 'clotilde.carvalhoso@email.com',    'Clotilde Carvalhoso',             'BA', 'parda'      , 'F', 'Silver')
                ,('237.679.884-10', '(91) 95606-8233', 'clovis.pamplona@email.com',        'Clóvis Pamplona',                 'PB', 'parda'      , 'M', 'Silver')
                ,('340.142.646-02', '(37) 98143-9890', 'constanca.alcaide@email.com',      'Constança Alcaide',               'GO', 'branca'     , 'M', 'Silver')
                ,('684.200.750-00', '(71) 94135-0992', 'constantino.pedroso@email.com',    'Constantino Pedroso',             'AC', 'indígena'   , 'M', 'Silver')
                ,('295.733.956-06', '(89) 98645-8085', 'corina.damaso@email.com',          'Corina Dâmaso',                   'CE', 'branca'     , 'F', 'Silver')
                ,('599.545.500-11', '(93) 92930-8094', 'cosme.ipanema@email.com',          'Cosme Ipanema',                   'PI', 'amarela'    , 'M', 'Silver')
                ,('398.209.671-55', '(73) 99051-3875', 'cosme.lira@email.com',             'Cosme Lira',                      'PR', 'parda'      , 'M', 'Silver')
                ,('238.123.044-06', '(28) 94075-7713', 'cosme.zambujal@email.com',         'Cosme Zambujal',                  'BA', 'indígena'   , 'M', 'Silver')
                ,('243.157.749-01', '(84) 92304-0049', 'crispim.cordero@email.com',        'Crispim Cordero',                 'SC', 'preta'      , 'M', 'Silver')
                ,('162.697.398-96', '(24) 99402-6022', 'crispim.macena@email.com',         'Crispim Macena',                  'AP', 'indígena'   , 'M', 'Silver')
                ,('501.927.731-27', '(24) 99510-5477', 'cristiana.campello@email.com',     'Cristiana Campello',              'RN', 'branca'     , 'F', 'Silver')
                ,('356.634.676-45', '(79) 92723-0502', 'custodio.goncalves@email.com',     'Custódio Gonçalves',              'DF', 'indígena'   , 'M', 'Silver')
                ,('866.041.934-03', '(32) 99295-2065', 'custodio.rolim@email.com',         'Custódio Rolim',                  'ES', 'indígena'   , 'M', 'Silver')
                ,('782.758.658-91', '(46) 97842-1638', 'david.bras@email.com',             'David Brás',                      'RJ', 'preta'      , 'M', 'Silver')
                ,('702.096.351-00', '(43) 92734-0168', 'david.carvalhais@email.com',       'David Carvalhais',                'RJ', 'indígena'   , 'M', 'Silver')
                ,('070.044.239-13', '(17) 94558-9145', 'davide.alcantara@email.com',       'Davide Alcántara',                'MA', 'preta'      , 'M', 'Silver')
                ,('885.925.372-15', '(32) 99263-3534', 'davide.fraga@email.com',           'Davide Fraga',                    'SC', 'branca'     , 'M', 'Silver')
                ,('142.137.076-01', '(97) 97793-7003', 'deise.farias@email.com',           'Deise Farias',                    'AM', 'preta'      , 'F', 'Silver')
                ,('895.793.400-64', '(95) 98702-6283', 'deise.laureano@email.com',         'Deise Laureano',                  'PI', 'branca'     , 'F', 'Silver')
                ,('342.166.952-05', '(28) 93939-2883', 'deise.meneses@email.com',          'Deise Meneses',                   'DF', 'preta'      , 'F', 'Silver')
                ,('098.291.150-54', '(92) 97964-9773', 'delia.chousa@email.com',           'Délia Chousa',                    'PB', 'parda'      , 'F', 'Silver')
                ,('090.665.886-16', '(34) 90777-9209', 'delio.paranhos@email.com',         'Délio Paranhos',                  'BA', 'amarela'    , 'M', 'Silver')
                ,('869.294.552-88', '(44) 95867-2454', 'deolinda.castelbranco@email.com',  'Deolinda Castelbranco',           'RO', 'amarela'    , 'F', 'Silver')
                ,('437.865.177-70', '(49) 95278-2901', 'deolinda.castella@email.com',      'Deolinda Castella',               'RJ', 'indígena'   , 'F', 'Silver')
                ,('152.922.068-81', '(24) 93640-0703', 'derli.lozada@email.com',           'Derli Lozada',                    'RJ', 'branca'     , 'F', 'Silver')
                ,('676.643.800-22', '(92) 97961-2892', 'diana.baptista@email.com',         'Diana Baptista',                  'AM', 'amarela'    , 'F', 'Silver')
                ,('898.431.892-21', '(85) 95814-6147', 'dinarte.mangueira@email.com',      'Dinarte Mangueira',               'DF', 'branca'     , 'F', 'Silver')
                ,('786.044.055-50', '(55) 90555-2299', 'dinarte.marino@email.com',         'Dinarte Marino',                  'MS', 'branca'     , 'F', 'Silver')
                ,('584.847.104-59', '(75) 98219-9100', 'dinarte.tabalipa@email.com',       'Dinarte Tabalipa',                'RS', 'preta'      , 'F', 'Silver')
                ,('962.869.893-15', '(64) 92521-9547', 'dinarte.tabares@email.com',        'Dinarte Tabares',                 'RR', 'indígena'   , 'F', 'Silver')
                ,('779.305.440-79', '(35) 93635-6868', 'diodete.carijo@email.com',         'Diodete Carijó',                  'PB', 'parda'      , 'F', 'Silver')
                ,('885.785.047-12', '(46) 98634-1621', 'diodete.queiroga@email.com',       'Diodete Queiroga',                'GO', 'preta'      , 'F', 'Silver')
                ,('277.868.915-01', '(46) 94607-1227', 'diogo.simon@email.com',            'Diogo Simón',                     'BA', 'branca'     , 'M', 'Silver')
                ,('890.624.176-39', '(51) 95028-3979', 'dionisio.saltao@email.com',        'Dionísio Saltão',                 'PR', 'preta'      , 'M', 'Gold'  )
                ,('686.831.368-33', '(73) 98886-7223', 'dora.galvao@email.com',            'Dora Galvão',                     'MT', 'parda'      , 'F', 'Silver')
                ,('633.600.372-86', '(53) 91614-0129', 'dora.rocha@email.com',             'Dora Rocha',                      'DF', 'branca'     , 'F', 'Silver')
                ,('314.836.716-23', '(85) 96986-8842', 'doroteia.quintanilla@email.com',   'Doroteia Quintanilla',            'RR', 'preta'      , 'F', 'Silver')
                ,('453.180.499-66', '(48) 93325-8828', 'duarte.sampaio@email.com',         'Duarte Sampaio',                  'RJ', 'branca'     , 'M', 'Silver')
                ,('388.613.188-27', '(94) 91580-1045', 'dulce.prado@email.com',            'Dulce Prado',                     'AC', 'indígena'   , 'F', 'Silver')
                ,('109.540.825-91', '(31) 96802-2105', 'dulce.tome@email.com',             'Dulce Tomé',                      'CE', 'amarela'    , 'F', 'Silver')
                ,('763.336.560-97', '(96) 93240-2276', 'eduarda.borba@email.com',          'Eduarda Borba',                   'MA', 'branca'     , 'F', 'Silver')
                ,('297.572.988-08', '(95) 98580-4044', 'eduarda.madureira@email.com',      'Eduarda Madureira',               'DF', 'parda'      , 'F', 'Silver')
                ,('821.164.443-65', '(98) 96199-5014', 'eladio.froes@email.com',           'Eládio Froes',                    'AM', 'indígena'   , 'M', 'Silver')
                ,('636.938.754-13', '(17) 99751-4683', 'eloi.meneses@email.com',           'Eloi Meneses',                    'CE', 'amarela'    , 'M', 'Silver')
                ,('863.836.413-01', '(33) 98448-1447', 'eloi.pereira@email.com',           'Eloi Pereira',                    'PA', 'indígena'   , 'M', 'Silver')
                ,('434.142.264-21', '(96) 94875-9402', 'eloi.vasques@email.com',           'Eloi Vasques',                    'RN', 'parda'      , 'M', 'Silver')
                ,('563.522.146-73', '(96) 99821-0655', 'elsa.alencar@email.com',           'Elsa Alencar',                    'RJ', 'indígena'   , 'F', 'Silver')
                ,('246.307.845-60', '(19) 94735-1815', 'elsa.barreto@email.com',           'Elsa Barreto',                    'MA', 'indígena'   , 'F', 'Silver')
                ,('247.104.218-03', '(99) 96564-3816', 'elvira.acores@email.com',          'Elvira Açores',                   'SC', 'indígena'   , 'F', 'Silver')
                ,('533.463.896-26', '(28) 92181-5520', 'ema.nieves@email.com',             'Ema Nieves',                      'GO', 'parda'      , 'F', 'Silver')
                ,('169.537.412-60', '(71) 97438-0373', 'emiliana.villalobos@email.com',    'Emiliana Villalobos',             'RO', 'amarela'    , 'F', 'Silver')
                ,('059.809.699-00', '(83) 98327-3762', 'epaminondas.sousa@email.com',      'Epaminondas Sousa de Arronches',  'MS', 'preta'      , 'M', 'Silver')
                ,('658.063.518-58', '(67) 92620-7072', 'ermelinda.casquero@email.com',     'Ermelinda Casquero',              'AM', 'indígena'   , 'F', 'Silver')
                ,('368.019.704-70', '(42) 95061-5724', 'estefania.cyrne@email.com',        'Estefânia Cyrne',                 'RO', 'parda'      , 'F', 'Silver')
                ,('301.457.485-61', '(93) 95905-5293', 'estela.mattos@email.com',          'Estela Mattos',                   'PI', 'indígena'   , 'F', 'Silver')
                ,('962.118.814-87', '(75) 98021-6170', 'ester.castanho@email.com',         'Ester Castanho',                  'AC', 'preta'      , 'F', 'Silver')
                ,('585.256.248-37', '(28) 90462-9463', 'ester.dantas@email.com',           'Ester Dantas',                    'SP', 'indígena'   , 'F', 'Silver')
                ,('726.406.890-02', '(87) 94007-2202', 'estevao.simao@email.com',          'Estêvão Simão',                   'DF', 'preta'      , 'M', 'Silver')
                ,('991.145.568-80', '(97) 96946-9101', 'eusebio.bairros@email.com',        'Eusébio Bairros',                 'SP', 'amarela'    , 'M', 'Silver')
                ,('491.884.555-05', '(98) 95872-8956', 'eusebio.mata@email.com',           'Eusébio Mata',                    'PI', 'indígena'   , 'M', 'Silver')
                ,('546.764.339-80', '(62) 90091-8059', 'eusebio.pacheco@email.com',        'Eusébio Pacheco',                 'TO', 'amarela'    , 'M', 'Silver')
                ,('905.525.212-30', '(48) 94940-3856', 'eusebio.salomao@email.com',        'Eusébio Salomão',                 'AC', 'parda'      , 'M', 'Silver')
                ,('462.290.942-15', '(68) 93017-2775', 'evaristo.bahia@email.com',         'Evaristo Bahía',                  'AC', 'preta'      , 'M', 'Silver')
                ,('249.336.480-50', '(98) 90414-0367', 'fabiano.bethancout@email.com',     'Fabiano Bethancout',              'PB', 'preta'      , 'M', 'Silver')
                ,('508.472.934-67', '(24) 93746-2320', 'fabricio.varella@email.com',       'Fabrício Varella',                'AM', 'branca'     , 'M', 'Silver')
                ,('640.605.938-77', '(83) 91179-0889', 'fabricio.vidal@email.com',         'Fabrício Vidal',                  'AL', 'preta'      , 'M', 'Silver')
                ,('163.711.135-50', '(62) 95135-4974', 'faustino.maranhao@email.com',      'Faustino Maranhão',               'PI', 'parda'      , 'M', 'Silver')
                ,('375.548.790-06', '(45) 94808-5228', 'fausto.miranda@email.com',         'Fausto Miranda',                  'AP', 'parda'      , 'M', 'Silver')
                ,('759.303.673-10', '(16) 95560-2825', 'fausto.montenegro@email.com',      'Fausto Montenegro',               'SE', 'preta'      , 'M', 'Silver')
                ,('422.180.676-17', '(53) 93660-0206', 'feliciana.silvera@email.com',      'Feliciana Silvera',               'AC', 'branca'     , 'F', 'Silver')
                ,('815.117.524-91', '(82) 94310-7748', 'feliciano.franca@email.com',       'Feliciano Franca',                'PA', 'amarela'    , 'M', 'Silver')
                ,('370.723.551-38', '(65) 93234-8691', 'felicidade.aldea@email.com',       'Felicidade Aldea',                'SE', 'branca'     , 'F', 'Silver')
                ,('244.190.607-00', '(38) 99658-8912', 'filipa.mattozo@email.com',         'Filipa Mattozo',                  'MA', 'parda'      , 'F', 'Silver')
                ,('272.215.255-01', '(99) 96102-3592', 'filipe.lamego@email.com',          'Filipe Lamego',                   'AL', 'indígena'   , 'M', 'Silver')
                ,('423.707.386-69', '(48) 90201-8062', 'firmina.carrasco@email.com',       'Firmina Carrasco',                'TO', 'amarela'    , 'F', 'Silver')
                ,('838.751.796-84', '(79) 91917-9117', 'firmino.galvan@email.com',         'Firmino Galván',                  'MA', 'preta'      , 'M', 'Silver')
                ,('561.137.989-33', '(65) 97462-0422', 'firmino.meireles@email.com',       'Firmino Meireles',                'AM', 'branca'     , 'M', 'Gold')
                ,('819.890.992-38', '(86) 99063-6269', 'firmino.puentes@email.com',        'Firmino Puentes',                 'PB', 'parda'      , 'M', 'Silver')
                ,('018.475.667-74', '(83) 93537-5490', 'flaminia.miera@email.com',         'Flamínia Miera',                  'MT', 'branca'     , 'F', 'Silver')
                ,('126.059.010-09', '(31) 93216-9861', 'flavia.camacho@email.com',         'Flávia Camacho',                  'BA', 'branca'     , 'F', 'Silver')
                ,('131.812.820-06', '(98) 97407-7533', 'flavia.campos@email.com',          'Flávia Campos',                   'RR', 'preta'      , 'F', 'Silver')
                ,('133.308.118-97', '(83) 93369-5388', 'flor.ginjeira@email.com',          'Flor Ginjeira',                   'ES', 'preta'      , 'M', 'Silver')
                ,('757.511.498-07', '(89) 90218-5754', 'flor.vilanova@email.com',          'Flor Vilanova',                   'CE', 'amarela'    , 'M', 'Platinum')
                ,('194.123.202-72', '(51) 99231-5344', 'florencio.lameiras@email.com',     'Florêncio Lameiras',              'PR', 'parda'      , 'M', 'Silver')
                ,('750.722.289-67', '(67) 97137-5153', 'florencio.vieira@email.com',       'Florêncio Vieira',                'PA', 'indígena'   , 'M', 'Silver')
                ,('094.075.532-72', '(28) 94473-6980', 'floriano.orrica@email.com',        'Floriano Orriça',                 'AP', 'preta'      , 'M', 'Silver')
                ,('232.837.605-35', '(75) 97039-9302', 'floriano.siebra@email.com',        'Floriano Siebra',                 'MS', 'branca'     , 'M', 'Silver')
                ,('962.464.696-12', '(96) 99888-3597', 'florinda.assuncao@email.com',      'Florinda Assunção',               'PI', 'amarela'    , 'F', 'Silver')
                ,('645.500.566-06', '(41) 95805-7898', 'francisca.ramallo@email.com',      'Francisca Ramallo',               'RJ', 'branca'     , 'F', 'Silver')
                ,('594.204.900-92', '(54) 93604-4759', 'francisca.teodoro@email.com',      'Francisca Teodoro',               'SE', 'amarela'    , 'F', 'Silver')
                ,('994.005.108-53', '(55) 99720-2025', 'francisco.medina@email.com',       'Francisco Medina',                'PA', 'amarela'    , 'M', 'Silver')
                ,('150.816.801-68', '(17) 92597-5508', 'galindo.bettencourt@email.com',    'Galindo Bettencourt',             'RN', 'indígena'   , 'M', 'Silver')
                ,('313.997.619-43', '(89) 96865-0294', 'garibaldo.oleiro@email.com',       'Garibaldo Oleiro',                'RO', 'indígena'   , 'M', 'Silver')
                ,('285.100.146-98', '(42) 96088-0292', 'gaudencio.ipiranga@email.com',     'Gaudêncio Ipiranga',              'RR', 'amarela'    , 'M', 'Silver')
                ,('194.498.815-70', '(19) 93843-7119', 'germana.conde@email.com',          'Germana Conde',                   'AM', 'preta'      , 'F', 'Silver')
                ,('922.499.348-60', '(68) 99583-7364', 'gertrudes.hidalgo@email.com',      'Gertrudes Hidalgo',               'PA', 'preta'      , 'F', 'Silver')
                ,('031.984.980-58', '(89) 99340-1108', 'gertrudes.infante@email.com',      'Gertrudes Infante',               'RS', 'indígena'   , 'F', 'Silver')
                ,('865.923.320-42', '(88) 92107-9067', 'gertrudes.rabello@email.com',      'Gertrudes Rabello',               'SC', 'preta'      , 'F', 'Silver')
                ,('543.516.936-49', '(66) 94764-7653', 'gilberto.moutinho@email.com',      'Gilberto Moutinho',               'PB', 'parda'      , 'M', 'Silver')
                ,('657.747.282-34', '(46) 95476-6942', 'gilda.parente@email.com',          'Gilda Parente',                   'RR', 'indígena'   , 'F', 'Silver')
                ,('395.708.019-30', '(46) 92812-4566', 'gineculo.luz@email.com',           'Ginéculo Luz',                    'RJ', 'amarela'    , 'M', 'Silver')
                ,('307.240.058-03', '(68) 98880-7890', 'gisela.bahia@email.com',           'Gisela Bahia',                    'SP', 'amarela'    , 'F', 'Silver')
                ,('901.473.016-02', '(38) 97819-2846', 'gisela.candeias@email.com',        'Gisela Candeias',                 'SC', 'indígena'   , 'F', 'Silver')
                ,('493.425.789-69', '(37) 90464-6986', 'glauber.guedella@email.com',       'Gláuber Guedella',                'RS', 'parda'      , 'M', 'Silver')
                ,('800.168.000-28', '(42) 90230-7600', 'glaucia.canela@email.com',         'Gláucia Canela',                  'RO', 'preta'      , 'F', 'Silver')
                ,('155.124.918-90', '(91) 99277-2174', 'godinho.ou@email.com',             'Godinho ou Godim Fogaça',         'RN', 'parda'      , 'M', 'Silver')
                ,('294.294.581-83', '(38) 96420-5216', 'godinho.ou@email.com',             'Godinho ou Godim Jácome',         'RN', 'branca'     , 'M', 'Silver')
                ,('611.500.524-81', '(82) 92185-1597', 'godofredo.mascarenas@email.com',   'Godofredo Mascareñas',            'SP', 'parda'      , 'M', 'Silver')
                ,('871.956.767-73', '(38) 91962-2655', 'godofredo.quiroga@email.com',      'Godofredo Quiroga',               'BA', 'indígena'   , 'M', 'Silver')
                ,('420.934.706-08', '(82) 93841-7808', 'gomes.hurtado@email.com',          'Gomes Hurtado',                   'AC', 'branca'     , 'M', 'Silver')
                ,('522.975.723-01', '(98) 99556-8961', 'goncalo.figueiro@email.com',       'Gonçalo Figueiró',                'PI', 'parda'      , 'M', 'Silver')
                ,('228.985.044-67', '(43) 98481-7957', 'graca.tabosa@email.com',           'Graça Tabosa',                    'RN', 'amarela'    , 'F', 'Silver')
                ,('913.484.325-60', '(21) 95304-2420', 'greice.lameirinhas@email.com',     'Greice Lameirinhas',              'PI', 'parda'      , 'F', 'Silver')
                ,('143.701.445-31', '(94) 95727-1942', 'guadalupe.rodrigues@email.com',    'Guadalupe Rodrigues',             'SC', 'indígena'   , 'F', 'Silver')
                ,('182.372.689-54', '(69) 94939-5830', 'guadalupe.telinhos@email.com',     'Guadalupe Telinhos',              'AL', 'parda'      , 'F', 'Silver')
                ,('509.039.540-36', '(48) 98459-5096', 'guaraci.imbassai@email.com',       'Guaraci Imbassaí',                'PB', 'preta'      , 'F', 'Silver')
                ,('024.397.858-81', '(98) 94085-8860', 'gueda.cartaxo@email.com',          'Gueda Cartaxo',                   'MS', 'indígena'   , 'F', 'Silver')
                ,('317.617.649-00', '(93) 98274-6257', 'gueda.laureano@email.com',         'Gueda Laureano',                  'BA', 'branca'     , 'F', 'Silver')
                ,('970.619.452-57', '(37) 98760-9237', 'guida.beiriz@email.com',           'Guida Beiriz',                    'AP', 'branca'     , 'F', 'Silver')
                ,('333.238.960-25', '(46) 92683-8830', 'guilhermina.hurtado@email.com',    'Guilhermina Hurtado',             'PA', 'branca'     , 'F', 'Silver')
                ,('124.303.494-77', '(63) 95289-6125', 'guilhermina.vilaca@email.com',     'Guilhermina Vilaça',              'BA', 'parda'      , 'F', 'Silver')
                ,('547.366.983-22', '(71) 94130-7846', 'hedviges.lessa@email.com',         'Hedviges Lessa',                  'MG', 'branca'     , 'F', 'Silver')
                ,('537.558.834-06', '(66) 99552-5130', 'helder.brion@email.com',           'Hélder Brión',                    'PB', 'amarela'    , 'M', 'Silver')
                ,('532.750.067-54', '(95) 91198-4708', 'helder.valentin@email.com',        'Hélder Valentín',                 'AL', 'branca'     , 'M', 'Silver')
                ,('702.833.805-35', '(18) 99333-7443', 'heloisa.cayubi@email.com',         'Heloísa Cayubi',                  'MS', 'preta'      , 'F', 'Silver')
                ,('542.321.299-52', '(82) 99429-5026', 'herberto.castro@email.com',        'Herberto Castro',                 'MA', 'preta'      , 'M', 'Silver')
                ,('963.482.178-25', '(97) 91299-7269', 'herberto.pari@email.com',          'Herberto Pari',                   'BA', 'indígena'   , 'M', 'Silver')
                ,('899.883.926-13', '(49) 96321-6922', 'herculano.viveiros@email.com',     'Herculano Viveiros',              'RN', 'preta'      , 'M', 'Silver')
                ,('872.539.435-54', '(88) 95350-3064', 'hermano.diegues@email.com',        'Hermano Diegues',                 'ES', 'parda'      , 'M', 'Silver')
                ,('298.123.720-91', '(17) 93090-7094', 'hermigio.cezar@email.com',         'Hermígio Cezar',                  'SP', 'branca'     , 'M', 'Silver')
                ,('108.591.497-65', '(45) 96742-2069', 'hermigio.villaverde@email.com',    'Hermígio Villaverde',             'MA', 'indígena'   , 'M', 'Silver')
                ,('515.548.535-59', '(18) 90517-9612', 'honorina.camarinho@email.com',     'Honorina Camarinho',              'PR', 'amarela'    , 'F', 'Silver')
                ,('391.766.574-34', '(49) 91045-5194', 'honorina.villaverde@email.com',    'Honorina Villaverde',             'PE', 'branca'     , 'F', 'Gold')
                ,('354.727.623-32', '(65) 98379-4868', 'honorio.mafra@email.com',          'Honório Mafra',                   'RR', 'branca'     , 'M', 'Silver')
                ,('060.202.538-99', '(92) 90227-2746', 'horacio.pires@email.com',          'Horácio Pires',                   'MG', 'amarela'    , 'M', 'Silver')
                ,('274.815.570-04', '(98) 90890-1938', 'hugo.covilha@email.com',           'Hugo Covilhã',                    'CE', 'indígena'   , 'M', 'Silver')
                ,('813.975.196-06', '(42) 99411-3877', 'humberto.almeida@email.com',       'Humberto Almeida',                'AC', 'amarela'    , 'M', 'Silver')
                ,('034.265.877-85', '(88) 98194-0759', 'humberto.lemes@email.com',         'Humberto Lemes',                  'BA', 'preta'      , 'M', 'Silver')
                ,('393.507.821-89', '(69) 90995-7605', 'humberto.morgado@email.com',       'Humberto Morgado',                'SP', 'amarela'    , 'M', 'Silver')
                ,('690.175.659-34', '(51) 92030-1486', 'humberto.vergueiro@email.com',     'Humberto Vergueiro',              'MG', 'preta'      , 'M', 'Silver')
                ,('190.932.299-74', '(99) 90445-3358', 'ibijara.botelho@email.com',        'Ibijara Botelho',                 'RR', 'parda'      , 'F', 'Platinum')
                ,('622.783.526-95', '(86) 93297-8927', 'ifigenia.lustosa@email.com',       'Ifigénia Lustosa',                'PE', 'preta'      , 'F', 'Silver')
                ,('577.715.490-55', '(62) 98016-2128', 'ifigenia.pires@email.com',         'Ifigénia Pires',                  'PA', 'preta'      , 'F', 'Silver')
                ,('451.852.611-29', '(71) 99253-0791', 'ilduara.chavez@email.com',         'Ilduara Chávez',                  'MT', 'preta'      , 'F', 'Silver')
                ,('626.954.036-40', '(17) 90893-1765', 'ines.neres@email.com',             'Inês Neres',                      'CE', 'indígena'   , 'F', 'Silver')
                ,('729.159.979-26', '(84) 99078-4450', 'ingrit.mayor@email.com',           'Ingrit Mayor',                    'SC', 'preta'      , 'M', 'Silver')
                ,('837.901.575-46', '(18) 90062-5390', 'iolanda.rabello@email.com',        'Iolanda Rabello',                 'PB', 'parda'      , 'F', 'Silver')
                ,('319.914.289-36', '(17) 97154-5407', 'iracema.rodriguez@email.com',      'Iracema Rodríguez',               'BA', 'indígena'   , 'F', 'Gold')
                ,('181.468.421-27', '(24) 93025-3031', 'iraci.alcoforado@email.com',       'Iraci Alcoforado',                'MS', 'parda'      , 'F', 'Silver')
                ,('983.933.937-01', '(89) 92519-7486', 'irani.jaguariuna@email.com',       'Irani Jaguariúna',                'AM', 'parda'      , 'F', 'Silver')
                ,('938.507.055-01', '(68) 95288-0667', 'irene.meireles@email.com',         'Irene Meireles',                  'MS', 'branca'     , 'F', 'Silver')
                ,('532.163.656-70', '(16) 91177-6456', 'irene.villanueva@email.com',       'Irene Villanueva',                'AC', 'indígena'   , 'F', 'Silver')
                ,('092.495.407-87', '(54) 91043-0956', 'isabel.meirelles@email.com',       'Isabel Meirelles',                'RO', 'amarela'    , 'F', 'Silver')
                ,('153.597.344-77', '(28) 92536-1099', 'israel.canela@email.com',          'Israel Canela',                   'RN', 'amarela'    , 'M', 'Silver')
                ,('311.735.382-83', '(75) 91428-9743', 'iuri.guterres@email.com',          'Iuri Guterres',                   'GO', 'branca'     , 'M', 'Silver')
                ,('042.517.751-39', '(32) 96150-2432', 'jacinto.dorneles@email.com',       'Jacinto Dorneles',                'MG', 'branca'     , 'M', 'Silver')
                ,('621.749.876-60', '(96) 92589-0341', 'jandaira.albuquerque@email.com',   'Jandaíra Albuquerque',            'SP', 'parda'      , 'F', 'Silver')
                ,('789.445.352-80', '(49) 97702-4713', 'joana.atai@email.com',             'Joana Ataí',                      'GO', 'preta'      , 'F', 'Platinum')
                ,('181.363.970-15', '(86) 92069-1418', 'joaquim.hurtado@email.com',        'Joaquim Hurtado',                 'AP', 'amarela'    , 'M', 'Silver')
                ,('494.501.461-28', '(77) 97195-4161', 'joaquim.mieiro@email.com',         'Joaquim Mieiro',                  'TO', 'preta'      , 'M', 'Silver')
                ,('747.610.654-78', '(44) 98015-0608', 'joaquina.vasconcelos@email.com',   'Joaquina Vasconcelos',            'SC', 'preta'      , 'F', 'Silver');
            `);
        }
        const [produtos] = await conn.query(`SELECT id_produto FROM produtos limit 1`);
        console.log('migrations() => produtos OK');
        if (produtos.length < 1) {
            await conn.query(`
                INSERT INTO produtos(produto, preco)
                    VALUES ('Bicicleta Aro 29 Mountain Bike Endorphine 6.3 - 24 Marchas - Shimano - Alumínio', 8852.00)
                        ,('Bicicleta Altools Stroll Aro 26 Freio À Disco 21 Marchas', 9201.00)
                        ,('Bicicleta Gts Advanced 1.0 Aro 29 Freio Disco Câmbio Traseiro Shimano 24 Marchas', 4255.00)
                        ,('Bicicleta Trinc Câmbios Shimano Aro 29 Freio A Disco 24v', 7658.00)
                        ,('Bicicleta Gometws Endorphine 7.3 - Shimano Alumínio Aro 29 - 24 Marchas', 2966.00)
                        ,('Bicicleta Gometws Endorphine 6.1 Shimano Alumínio- Aro 26 - 21 Marchas', 2955.00)
                        ,('Capacete Gometws Endorphine 2.0', 155.00)
                        ,('Luva De Ciclismo - Gometws Sports', 188.00)
                        ,('Bermuda Predactor 3Xu Pro', 115.00)
                        ,('Camiseta Predactor 3Xu Multiplied', 135.00);
            `);
        }
        const [vendedores] = await conn.query(`SELECT id_vendedor FROM vendedores limit 1`);
        console.log('migrations() => vendedores OK');
        if (vendedores.length < 1) {
            await conn.query(`
                INSERT INTO
                    vendedores
                        (cpf, telefone, email, origem_racial, nome)
                    VALUES
                        ('523.120.480-35', '(88) 90334-0392', 'armando_lago@lojasimulada.com.br',     'branca',   'Armando Lago'     )
                        ,('404.314.222-65', '(94) 94677-8772', 'capitolino_bahia@lojasimulada.com.br', 'amarela',  'Capitolino Bahía' )
                        ,('017.474.393-98', '(31) 93436-6016', 'daniel_piraja@lojasimulada.com.br',    'preta',    'Daniel Pirajá'    )
                        ,('295.738.270-93', '(75) 96625-3591', 'godo_capiperibe@lojasimulada.com.br',  'amarela',  'Godo Capiperibe'  )
                        ,('543.492.100-32', '(33) 97918-9825', 'helio_liberato@lojasimulada.com.br',   'branca',   'Hélio Liberato'   )
                        ,('416.552.263-55', '(22) 93277-8907', 'ibere_lacerda@lojasimulada.com.br',    'indígena', 'Iberê Lacerda'    )
                        ,('750.702.056-84', '(65) 92445-7359', 'jessica_castelao@lojasimulada.com.br', 'branca',   'Jéssica Castelão' )
                        ,('981.475.880-97', '(18) 90653-1009', 'napoleao_mendez@lojasimulada.com.br',  'indígena', 'Napoleão Méndez'  )
                        ,('075.959.284-51', '(19) 96086-8436', 'simao_rivero@lojasimulada.com.br',     'preta',    'Simão Rivero'     )
                        ,('646.651.995-31', '(24) 91196-2261', 'tobias_furtado@lojasimulada.com.br',   'indígena', 'Tobias Furtado'   );
            `);
        }
        await desconectar();
        return        
    } catch (error) {
        await delay(1000)
        await migrations()
    }
}

migrations()

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