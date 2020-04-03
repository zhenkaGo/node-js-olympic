const DB = require('../database')

const args = process.argv.slice(2)
const seasons = new Map([['winter', 1], ['summer', 0]])
const medals = new Map([['gold', 1], ['silver', 2], ['bronze', 3]])

const params = {
  season: null, // is_required
  noc: null, // is_required
  medal: null
}
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
  params.noc = arg.toUpperCase()
}

if (params.season === null) console.log('Season is required argument')
if (!params.noc) console.log('NOC is required argument')

DB.all(`
  SELECT year, COUNT(medal) amount FROM games
  LEFT JOIN (
    SELECT game_id, medal FROM teams
      LEFT JOIN athletes ON athletes.team_id = teams.id
      INNER JOIN (
        SELECT medal, athlete_id, game_id FROM results WHERE medal ${params.medal ? `=${params.medal}` : 'IN (1, 2, 3)'}
      ) AS join_results ON join_results.athlete_id = athletes.id
      INNER JOIN (
        SELECT id, season FROM games WHERE season = ${params.season}
      ) AS join_games ON join_games.id = join_results.game_id
      WHERE
        noc_name = '${params.noc}'
  ) AS join_team_results ON games.id = join_team_results.game_id
  GROUP BY year
  ORDER BY year ASC
`, (err, rows) => {
  if (err) console.log(err, 'Err select')
  const data = rows.map(i => ([i.year, i.amount]))
  draw(data, ['year', 'amount'])
})

function draw(data, columns) {
  const max = Math.max(...data.map(arr => arr[1]))
  console.log(columns.join('\t'))
  for (const row of data) {
    const num = Math.round(row[1] / max * 100)
    console.log(`${row[0]}\t${'█'.repeat(num)}`)
  }
}
