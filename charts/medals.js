const DB = require('../database')
const { getParams, draw } = require('./utils')

const params = getParams('noc')

if (params.season === null) {
  console.log('Season is required argument')
  return
}
if (!params.noc) {
  console.log('NOC is required argument')
  return
}

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
        noc_name = UPPER('${params.noc}')
  ) AS join_team_results ON games.id = join_team_results.game_id
  GROUP BY year
  ORDER BY year ASC
`, (err, rows) => {
  if (err) console.log(err, 'Err select medals')
  const data = rows.map(i => ([i.year, i.amount]))
  draw(data, ['year', 'amount'])
})
