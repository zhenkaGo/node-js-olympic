const fs = require('fs')

// function parseCSVFile (filePatch) {
//   const promise = new Promise(resolve => {
//     const dataFileStream = fs.createReadStream(filePatch, 'utf8');
//     let dataStr = ''
//     dataFileStream.on('data', (chunk) => {
//       dataStr = dataStr + chunk
//     })
//     dataFileStream.on('end', () => {
//       const rows = dataStr.split('\r\n').map(i => i.split(/\s*,(?=(?:[^"]|"[^"]*")*$)\s*/g).map(i => i.replace(/"/g, '')))
//
//       resolve({
//         columns: rows[0],
//         data: rows.slice(1, rows.length - 1)
//       })
//     })
//   })
//   return promise
// }

function parseCSVFile (filePatch) {
  const promise = new Promise(resolve => {
    fs.readFile(filePatch, 'utf8', (err, data) => {
      const rows = data.split('\r\n').map(i => i.split(/\s*,(?=(?:[^"]|"[^"]*")*$)\s*/g))
      resolve({
        columns: rows[0],
        data: rows.slice(1, rows.length - 1)
      })
    })
  })
  return promise
}

module.exports = parseCSVFile
