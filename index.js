const fs = require('fs');
const path = require('path');
const csv = require('fast-csv');

const dicionarioEstado = require('./data/dicionarioEstado.json')
const dicionarioPais = require('./data/dicionarioPais.json')

let totalLinhasInvalidas = 0;
let dtNaciInvalidas = 0;
let dtObitoInvalidas = 0;
let naturalMissing = 0;
let naturalNaoInformado = 0;
var csvFinal = [['DTNASC', 'DTOBITO', 'NATURAL']];

//prepara o óbito para ser inserido no array csvFinal
function prepararObjeto(obito) {
    obito = [formatDate(obito.DTNASC), formatDate(obito.DTOBITO), formatPaisEstado(obito.NATURAL)]
    csvFinal.push(obito)
    return obito
}

//formata as datas
function formatDate(data) {
    data = data.split('')
    return data = `${data[0]}${data[1]}/${data[2]}${data[3]}/${data[4]}${data[5]}${data[6]}${data[7]}`
}

//formata o país e estado e caso não seja do Brasil preenche com páis do código
function formatPaisEstado(codigo) {
    if (codigo[0] == 8) {
        var estado = codigo.slice(1)
        return `Brasil - ${dicionarioEstado[estado]}`
    }

    if (codigo == '999') {
        naturalNaoInformado ++;
        return `Não informado`
    }

    return `${dicionarioPais[codigo]}`
}

//contabiliza os dados inválidos da base
function contabilizarInvalidos(obitoInvalido) {
    totalLinhasInvalidas ++

    if(obitoInvalido.DTNASC == ''){
        dtNaciInvalidas++
    } 

    if(obitoInvalido.DTOBITO == ''){
        dtObitoInvalidas++
    } 

    if(obitoInvalido.NATURAL == ''){
        naturalMissing++
    }
}

//salva o CSV
function salvarCSV(csvFinal) {
    csv.writeToPath(path.resolve(__dirname, 'data', 'RealatorioFinal.csv'), csvFinal)
    .on('error', err => console.error(err))
    .on('finish', () => console.log('Done writing.'));
}

//análisa e válida os dados do csv informado
fs.createReadStream(path.resolve(__dirname, 'data', 'relatorioAnalise.csv'))
.pipe(csv.parse({ headers: true, ignoreEmpty:   true  }))
.validate((data) => data.DTNASC !== '' && data.DTOBITO !== '' && data.NATURAL !== '')
.on('error', (error) => console.error(error))
.on('data', (row) => prepararObjeto(row))
.on('data-invalid', (row, rowNumber) => contabilizarInvalidos(row))
.on('end', (rowCount) => {
    salvarCSV(csvFinal)
    console.log(`Relatório \n Total de linhas inválidas: ${totalLinhasInvalidas} \n Datas de nascimento vazias: ${dtNaciInvalidas} \n Datas de óbitos vazias: ${dtObitoInvalidas} \n Natural missing: ${naturalMissing} \n Natural não informado: ${naturalNaoInformado} \n Total de linhas: ${rowCount}`)
});