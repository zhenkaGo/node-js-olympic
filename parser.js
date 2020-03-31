const fs = require('fs')

function CSVToArray(strData){
  const objPattern = /(,|\r?\n|\r|^)(?:"([^"]*(?:""[^"]*)*)"|([^",\r\n]*))/gi
  const arrData = [[]]
  let arrMatches = objPattern.exec(strData)
  while (arrMatches){
    const strMatchedDelimiter = arrMatches[1]
    if (strMatchedDelimiter.length && strMatchedDelimiter !== ',') {
      arrData.push([])
    }
    const strMatchedValue = arrMatches[2] || arrMatches[3]
    arrData[arrData.length - 1].push(strMatchedValue)
    arrMatches = objPattern.exec(strData)
  }
  return arrData
}

function parseCSVFile (filePatch) {
  return new Promise(resolve => {
    fs.readFile(filePatch, 'utf8', (err, data) => {
      const rows = CSVToArray(data)
      resolve({
         columns: rows[0],
         data: rows.slice(1, rows.length - 1)
       })
    })
  })
}

module.exports = parseCSVFile
