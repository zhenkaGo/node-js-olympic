const seasons = new Map([['winter', 1], ['summer', 0]])
const medals = new Map([['gold', 1], ['silver', 2], ['bronze', 3]])

const params = {
  season: null,
  noc: null,
  medal: null,
  year: null
}
const getParams = extraArg => {
  const args = process.argv.slice(2)
  for (let arg of args) {
    arg.toLowerCase()
    if (seasons.has(arg)) {
      params.season = seasons.get(arg)
      continue
    }
    if (medals.has(arg)) {
      params.medal = medals.get(arg)
      continue
    }
    params[extraArg] = arg
  }
  return params
}

const draw = (data, columns) => {
  const barLength = 150
  const max = Math.max(...data.map(arr => arr[1]))
  console.log(columns.join('\t'))
  for (const row of data) {
    const num = Math.round(row[1] / max * barLength)
    console.log(`${row[0]}\t${'█'.repeat(num)}`)
  }
}

module.exports = { getParams, draw }
