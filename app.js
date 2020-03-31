const sqlite3 = require('sqlite3').verbose();
const parser = require('./parser')

const SEASON = {
  Summer: 0,
  Winter: 1,
}
const MAX_VARIABLES = 999

class App {
  constructor({ databasePath }) {
    this.db = new sqlite3.Database(databasePath, sqlite3.OPEN_READWRITE, (err) => {
      if (err) {
        console.error(err.message);
      }
      console.log('Connected to the database.')
    })
  }

  closeDB() {
    this.db.close((err) => {
      if (err) {
        return console.error(err.message)
      }
      console.log('Close the database connection.')
    })
  }

  clearTables() {
    return new Promise((resolve, reject) => {
      const tables = ['athletes', 'teams', 'games', 'sports', 'events', 'results']
      let sql = ``
      for (const table of tables) {
        sql = `${sql} DELETE FROM ${table}; VACUUM; DELETE FROM sqlite_sequence WHERE name = '${table}';`
      }
      this.db.exec(sql, (err) => {
        if (err) {
          reject()
          return console.log(err, 'clearTables error =============')
        }
        console.log(`Tables has been cleared`)
        resolve()
      })
    })
  }

  async insertTeams() {
    console.time('createTeamMap')
    const teamTable = new Map()
    for (const row of this.data.data) {
      if (!teamTable.has(row[this.columnsOfCsv.get('NOC')]) && row[this.columnsOfCsv.get('NOC')] && row[this.columnsOfCsv.get('Team')]) {
        teamTable.set(row[this.columnsOfCsv.get('NOC')], row[this.columnsOfCsv.get('Team')])
      }
    }
    console.timeEnd('createTeamMap')
    const promiseInsert = new Promise(resolve => {
      this.db.serialize(() => {
        const stmt = this.db.prepare("INSERT INTO teams(name, noc_name) VALUES (?, ?)")
        for (const team of teamTable) {
          stmt.run([team[1], team[0]])
        }
        stmt.finalize(() => {
          console.log('teams finalize')
          resolve()
        })
      })
    })
    return promiseInsert
  }

  // async insertTeams() {
  //   console.time('createTeamMap')
  //   const teamTable = new Map()
  //   for (const row of this.data.data) {
  //     if (!teamTable.has(row[this.columnsOfCsv.get('"NOC"')]) && row[this.columnsOfCsv.get('"NOC"')] && row[this.columnsOfCsv.get('"Team"')]) {
  //       teamTable.set(row[this.columnsOfCsv.get('"NOC"')], row[this.columnsOfCsv.get('"Team"')])
  //     }
  //   }
  //   console.timeEnd('createTeamMap')
  //   const promiseInsert = new Promise(resolve => {
  //     const step = Math.floor(MAX_VARIABLES / 2)
  //     const placeholders = Array(2).fill('?').join(',')
  //     const variables = Array.from(teamTable).map(team => ([team[1], team[0]]))
  //
  //     for (let i = 0; i < variables.length; i += step) {
  //       const valueChunk = variables.slice(i, i + step)
  //       const valuesPlaceholder = valueChunk.map(() => (`(${placeholders})`))
  //       const params = valueChunk.flat()
  //       this.db.run(`INSERT INTO teams(name, noc_name) VALUES ${valuesPlaceholder.join(',')};`, params, (err) => {
  //         if (err) {
  //           return console.log(err, `err ========== teams`)
  //         }
  //         console.log('Teams has been inserted')
  //         resolve()
  //       })
  //     }
  //   })
  //   return promiseInsert
  // }

  async insertAthletes() {
    const athletesTable = new Map()
    for (const row of this.data.data) {
      if (!athletesTable.has(row[this.columnsOfCsv.get('ID')])) {
        const params = {}
        if (row[this.columnsOfCsv.get('Weight')] !== 'NA') {
          params.weight = row[this.columnsOfCsv.get('Weight')]
        }
        if (row[this.columnsOfCsv.get('Height')] !== 'NA') {
          params.height = row[this.columnsOfCsv.get('Height')]
        }
        athletesTable.set(row[this.columnsOfCsv.get('ID')], {
          full_name: row[this.columnsOfCsv.get('Name')].replace(/\(.*?\)/g, '').trim(),
          sex: row[this.columnsOfCsv.get('Sex')] ? row[this.columnsOfCsv.get('Sex')] : null,
          team: row[this.columnsOfCsv.get('NOC')],
          year_of_birth: row[this.columnsOfCsv.get('Age')] === 'NA'
            ? null
            : new Date().getFullYear() - row[this.columnsOfCsv.get('Age')],
          params: JSON.stringify(params),
        })
      }
    }
    const promiseAthletesInsert = new Promise(resolve => {
      this.db.serialize(() => {
        const stmt = this.db.prepare("INSERT INTO athletes(full_name, sex, params, year_of_birth, team_id) VALUES (?, ?, ?, ?, (SELECT id FROM teams WHERE noc_name = ?))")
        for (const athlete of athletesTable) {
          stmt.run([athlete[1].full_name, athlete[1].sex, athlete[1].params, athlete[1].year_of_birth, athlete[1].team], (err) => {
            if (err) {
              console.log(err, `err ========== ${athlete[0]}`)
            }
          })
        }
        stmt.finalize(() => {
          console.log('athlete finalize')
          resolve()
        })
      })
    })
    return await promiseAthletesInsert
  }

  async insertGames() {
    const gamesTable = new Map()
    for (const row of this.data.data) {
      if (row[this.columnsOfCsv.get('Games')] === '1906 Summer') {
        continue
      }
      if (gamesTable.has(row[this.columnsOfCsv.get('Games')])) {
        gamesTable.get(row[this.columnsOfCsv.get('Games')]).city.add(row[this.columnsOfCsv.get('City')])
      } else {
        gamesTable.set(row[this.columnsOfCsv.get('Games')], {
          year: row[this.columnsOfCsv.get('Year')],
          season: SEASON[row[this.columnsOfCsv.get('Season')]],
          city: new Set([row[this.columnsOfCsv.get('City')]])
        })
      }
    }

    const promiseGamesInsert = new Promise(resolve => {
      this.db.serialize(() => {
        const stmt = this.db.prepare("INSERT INTO games(year, season, city) VALUES (?, ?, ?)")
        for (const game of gamesTable) {
          stmt.run([game[1].year, game[1].season, Array.from(game[1].city).join(', ')], (err) => {
            if (err) {
              console.log(err, `err ========== ${game[0]}`)
            }
          })
        }
        stmt.finalize(() => {
          console.log('games finalize')
          resolve()
        })
      })
    })
    return promiseGamesInsert
  }

  async insertSports() {
    const sportsTable = new Set()
    for (const row of this.data.data) {
      sportsTable.add(row[this.columnsOfCsv.get('Sport')])
    }

      const promiseSportInsert = new Promise(resolve => {
      this.db.serialize(() => {
        const stmt = this.db.prepare("INSERT INTO sports(name) VALUES (?)")
        for (const sport of sportsTable) {
          stmt.run([sport], (err) => {
            if (err) {
              console.log(err, `err ========== ${sport}`)
            }
          })
        }
        stmt.finalize(() => {
          console.log('sports finalize')
          resolve()
        })
      })
    })
    return promiseSportInsert
  }

  insertEvents() {
    const eventsTable = new Set()
    for (const row of this.data.data) {
      eventsTable.add(row[this.columnsOfCsv.get('Event')])
    }

    const promiseSportInsert = new Promise(resolve => {
      this.db.serialize(() => {
        const stmt = this.db.prepare("INSERT INTO events(name) VALUES (?)")
        for (const event of eventsTable) {
          stmt.run([event], (err) => {
            if (err) {
              console.log(err, `err ========== ${event}`)
            }
          })
        }
        stmt.finalize(() => {
          console.log('event finalize')
          resolve()
        })
      })
    })
    return promiseSportInsert
  }

  async insertResults() {
    const MEDAL = {
      'NA': 0,
      'Gold': 1,
      'Silver': 2,
      'Bronze': 3,
    }
    const promiseSportInsert = new Promise(resolve => {
      this.db.serialize(() => {
        const stmt = this.db.prepare('INSERT INTO results(athlete_id, game_id, sport_id, event_id, medal) VALUES (' +
          '(SELECT id FROM athletes WHERE full_name = $full_name),' +
          '(SELECT id FROM games WHERE year = $year AND season = $season),' +
          '(SELECT id FROM sports WHERE name = $sport),' +
          '(SELECT id FROM events WHERE name = $event),' +
          '$medal' +
          ')')
        for (const row of this.data.data) {
          if (row[this.columnsOfCsv.get('Games')] === '1906 Summer') {
            continue
          }
          stmt.run({
            $full_name: row[this.columnsOfCsv.get('Name')].replace(/\(.*?\)/g, '').trim(),
            $year: row[this.columnsOfCsv.get('Year')],
            $season: SEASON[row[this.columnsOfCsv.get('Season')]],
            $sport: row[this.columnsOfCsv.get('Sport')],
            $event: row[this.columnsOfCsv.get('Event')],
            $medal: MEDAL[row[this.columnsOfCsv.get('Medal')]]
          }, (err) => {
            if (err) {
              console.log(err, `${row} err ==========`)
            }
          })
        }
        stmt.finalize(() => {
          console.log('results finalize')
          resolve()
        })
      })
    })
    return promiseSportInsert
  }

  async importData() {
    console.time('import')
    console.time('parser')
    this.data = await parser('athlete_events.csv')
    console.timeEnd('parser')
    this.columnsOfCsv = new Map()
    for (const col in this.data.columns) {
      this.columnsOfCsv.set(this.data.columns[col], col)
    }

    await this.clearTables()
    console.time('insertTeams')
    await this.insertTeams()
    console.timeEnd('insertTeams')

    // console.time('insertAthletes')
    // await this.insertAthletes()
    // console.timeEnd('insertAthletes')
    //
    // console.time('insertGames')
    // await this.insertGames()
    // console.timeEnd('insertGames')
    //
    // console.time('insertSports')
    // await this.insertSports()
    // console.timeEnd('insertSports')
    //
    // console.time('insertEvents')
    // await this.insertEvents()
    // console.timeEnd('insertEvents')

    // console.time('insertResults')
    // await this.insertResults()
    // console.timeEnd('insertResults')

    console.timeEnd('import')
  }
}


const app = new App({ databasePath: './olympic_history.db' })
app.importData().then(() => {
  app.closeDB()
})
