function abreviarSifra(v) {
    const valores = v?.toString().split(/\./);
    let strValor, response;
    if (valores?.length > 0)
        strValor = valores[0]
    if (strValor.length < 4) return strValor;
    const monet = ['K', 'M', 'MM', 'MM+'];
    for (let i = 3, m = 0; i < strValor.length; i = i + 3, m++) {
        response = `${strValor.slice((-3-i), -i)} ${monet[m > 3 ? 3 : m]}`
    }
    return response
}

function formatarSifra(valor) {
    if (isNaN(valor)) return valor;
    let strValor = Number(valor).toString()
    if (strValor.search(/\./g) < 0) strValor = strValor + '.00';
    strValor = strValor.replace(/\./g,',')
    console.log(strValor)
    if (strValor.length < 7) return strValor.replace(/([0-9]{0,3})(\,[0-9]{2})/g,'$1$2');
    if (strValor.length < 10) return strValor.replace(/([0-9]{0,3})([0-9]{3})(\,[0-9]{2})/g,'$1.$2$3');
    if (strValor.length < 13) return strValor.replace(/([0-9]{0,3})([0-9]{3})([0-9]{3})(\,[0-9]{2})/g,'$1.$2.$3$4');
    if (strValor.length < 16) return strValor.replace(/([0-9]{0,3})([0-9]{3})([0-9]{3})([0-9]{3})(\,[0-9]{2})/g,'$1.$2.$3.$4$5');
    if (strValor.length < 19) return strValor.replace(/([0-9]{0,3})([0-9]{3})([0-9]{3})([0-9]{3})([0-9]{3})(\,[0-9]{2})/g,'$1.$2.$3.$4.$5$6');
    if (strValor.length < 22) return strValor.replace(/([0-9]{0,3})([0-9]{3})([0-9]{3})([0-9]{3})([0-9]{3})([0-9]{3})(\,[0-9]{2})/g,'$1.$2.$3.$4.$5.$6$7');
    return strValor
}


esquema = StructType([
    StructField("quantidade_vendas", IntegerType(), False),
    StructField("maiores_vendas", StructType(esquema_maiores_vendas)),
])

esquema_maiores_vendas = StructType([
    StructField("faturamento", DoubleType(), False),
    StructField("desconto", DoubleType(), False),
    StructField("venda", DoubleType(), False),
])

esquema_maiores_vendas = StructType([
    StructField("id_vendedor", IntegerType(), False),
    StructField("valor_faturado", DoubleType(), False),
])

esquema_mais_vendidos = StructType([
    StructField("id_vendedor", IntegerType(), False),
    StructField("id_cliente", IntegerType(), False),
    StructField("id_produto", IntegerType(), False),
    StructField("id_venda", IntegerType(), False),
    StructField("quantidade", IntegerType(), False),
    StructField("valor_unitario", DoubleType(), False),
    StructField("valor_total", DoubleType(), False),
    StructField("desconto", DoubleType(), False),
])

const dataFromKafka = {
    quantidade_vendas: 'Integer',
    faturamento_desconto: [
        {
            faturamento: 'Double',
            desconto: 'Double',
            venda: 'Double',
        },
    ],
    maiores_vendas: [
        {
            id_vendedor: 'Integer',
            valor_faturado: 'Double',
        },
    ],
    mais_vendidos: [
        {
            id_vendedor: 'Integer',
            id_cliente: 'Integer',
            id_produto: 'Integer',
            id_venda: 'Integer',
            quantidade: 'Integer',
            valor_unitario: 'Double',
            valor_total: 'Double',
            desconto: 'Double',
        },
    ],
    data: 'Date',
}
const root =
{"quantidade_vendas":8.0,"data":"2024-05-30 23:23:00.000"}
const faturamento_desconto = 
{"faturamento_desconto":[{"faturamento":5.0,"desconto":6.0,"venda":7.0},{"faturamento":6.0,"desconto":7.0,"venda":8.0}]}
const maiores_vendas = 
{"maiores_vendas":[{"id_vendedor":1,"valor_faturado":10.0},{"id_vendedor":2,"valor_faturado":20.0}]}
const mais_vendidos = 
{"mais_vendidos":[{"id_vendedor":1,"id_cliente":1,"id_produto":1,"id_venda":1,"quantidade":1,"valor_unitario":1.01,"valor_total":1.01,"desconto":1.01},{"id_vendedor":2,"id_cliente":2,"id_produto":2,"id_venda":2,"quantidade":2,"valor_unitario":2.02,"valor_total":2.02,"desconto":2.02}]}

// {"json_data":"{\"qtdVenda\":21,\"grafico\":\"vendas\"}"}
// {"json_data":"{\"valor_total\":36.0,\"desconto\":9.0,\"faturamento\":27.0,\"grafico\":\"faturamento_desconto\"}"}
// {"json_data":"[{\"id_vendedor\":1,\"faturamento\":27.0,\"grafico\":\"maiores_vendas\"},{\"id_vendedor\":2,\"faturamento\":38.0,\"grafico\":\"maiores_vendas\"}]"}
// {"json_data":"[{\"id_produto\":1,\"quantidade\":9,\"valor_total\":36.0,\"desconto\":9.0,\"faturamento\":27.0,\"grafico\":\"mais_vendidos\"},{\"id_produto\":1,\"quantidade\":9,\"valor_total\":36.0,\"desconto\":9.0,\"faturamento\":27.0,\"grafico\":\"mais_vendidos\"}]"}



[{ id_vendedor: 1, faturamento: 27, grafico: 'maiores_vendas' },{ id_vendedor: 2, faturamento: 38, grafico: 'maiores_vendas' }]