const DB = require('../database')
const { getParams, draw } = require('./utils')

const params = getParams('year')

if (params.season === null) {
  console.log('Season is required argument')
  return
}

DB.connect()
DB.all(`
  SELECT noc_name noc, COUNT(medal) amount FROM teams
  LEFT JOIN (
    SELECT team_id, medal FROM athletes
    INNER JOIN (
      SELECT medal, athlete_id, game_id FROM results WHERE medal ${params.medal ? `=${params.medal}` : 'IN (1, 2, 3)'}
    ) AS join_results ON join_results.athlete_id = athletes.id
    INNER JOIN (
      SELECT id FROM games WHERE season = '${params.season}' ${params.year ? `AND year = ${params.year}` : ''}
    ) as join_games ON join_games.id = join_results.game_id
  ) AS join_athletes ON join_athletes.team_id = teams.id
  GROUP BY noc
  ORDER BY amount DESC
`)
  .then(rows => {
    const sum = rows.reduce((p, i) => {
      return p + i.amount
    }, 0)
    const avg = sum / rows.length
    const data = rows.map(i => ([i.noc, i.amount])).filter(i => i[1] > avg)
    draw(data, ['noc', 'amount'])
  })
  .catch(err => {
    console.error(err, 'Err select medals')
  })

