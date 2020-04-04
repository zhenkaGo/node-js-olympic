const parser = require('./parser')
const DB = require('./database')

const SEASON = {
  'Summer': 0,
  'Winter': 1,
}
const MAX_VARIABLES = 999
const MEDAL = {
  'NA': 0,
  'Gold': 1,
  'Silver': 2,
  'Bronze': 3,
}
const CURRENT_YEAR = new Date().getFullYear()

class Import {
  constructor() {
    this.columnsOfCsv = new Map()
    this.teamTable = new Map()
    this.athletesTable = new Map()
    this.gamesTable = new Map()
    this.sportsTable = new Set()
    this.eventsTable = new Set()
    this.resultTable = []
  }

  prepareDataStructure() {
    for (const row of this.data.data) {
      this.prepareTeams(row)
      this.prepareGames(row)
      this.sportsTable.add(row[this.columnsOfCsv.get('Sport')])
      this.eventsTable.add(row[this.columnsOfCsv.get('Event')])
    }
    for (const row of this.data.data) {
      this.prepareAthletes(row)
      this.prepareResults(row)
    }
  }

  async insertData(tableName, columns, variables) {
    return new Promise(resolve => {
      const step = Math.floor(MAX_VARIABLES / columns.length)
      const sqlQueries = []
      for (let i = 0; i < variables.length; i += step) {
        const valueChunk = variables.slice(i, i + step)
        const values = valueChunk.map((i) => (`(${Array.isArray(i) ? i.map(v => `"${v}"`).join(',') : `"${i}"`})`))
        sqlQueries.push(`INSERT INTO ${tableName}(${columns.join(',')}) VALUES ${values.join(',')}`)
      }
      DB.exec(sqlQueries.join(';'), err => {
        if (err) {
          return console.log(err, `err ========== ${tableName}`)
        }
        console.log(`${tableName} has been inserted`)
        resolve()
      })
    })
  }

  prepareTeams(row) {
    this.teamTable.set(row[this.columnsOfCsv.get('NOC')], row[this.columnsOfCsv.get('Team')])
  }

  prepareAthletes(row) {
    if (!this.athletesTable.has(row[this.columnsOfCsv.get('ID')])) {
      const age = row[this.columnsOfCsv.get('Age')]
      const sex = row[this.columnsOfCsv.get('Sex')]
      const weight = row[this.columnsOfCsv.get('Weight')]
      const height = row[this.columnsOfCsv.get('Height')]
      let params = `{${weight !== 'NA' ? `weight: ${weight}` : ''}${height !== 'NA' ? `, height: ${height}` : ''}}`
      this.athletesTable.set(row[this.columnsOfCsv.get('ID')], [
        row[this.columnsOfCsv.get('Name')].replace(/\(.*?\)/g, '').trim(),
        sex ? sex : null,
        params,
        age === 'NA' ? null : CURRENT_YEAR - age,
        Array.from(this.teamTable.keys()).indexOf(row[this.columnsOfCsv.get('NOC')]) + 1,
      ])
    }
  }

  prepareGames(row) {
    const city = row[this.columnsOfCsv.get('City')]
    const games = row[this.columnsOfCsv.get('Games')]
    if (games === '1906 Summer') {
      return
    }
    if (this.gamesTable.has(games)) {
      this.gamesTable.get(games)[2].add(city)
    } else {
      this.gamesTable.set(games, [
        row[this.columnsOfCsv.get('Year')],
        SEASON[row[this.columnsOfCsv.get('Season')]],
        new Set([city])
      ])
    }
  }

  prepareResults(row) {
    const games = row[this.columnsOfCsv.get('Games')]
    if (games === '1906 Summer') {
      return
    }

    this.resultTable.push([
      row[this.columnsOfCsv.get('ID')],
      Array.from(this.gamesTable.keys()).indexOf(games) + 1,
      Array.from(this.sportsTable).indexOf(row[this.columnsOfCsv.get('Sport')]) + 1,
      Array.from(this.eventsTable).indexOf(row[this.columnsOfCsv.get('Event')]) + 1,
      MEDAL[row[this.columnsOfCsv.get('Medal')]]
    ])
  }

  async importData() {
    console.time('import')
    this.data = await parser('./data/athlete_events.csv')
    for (const col in this.data.columns) {
      this.columnsOfCsv.set(this.data.columns[col], col)
    }
    this.prepareDataStructure()

    await DB.clearTables()
    await this.insertData('teams', ['noc_name', 'name'], Array.from(this.teamTable))

    await this.insertData(
      'athletes',
      ['full_name', 'sex', 'params', 'year_of_birth', 'team_id'],
      Array.from(this.athletesTable.values())
    )

    const games = Array.from(this.gamesTable.values()).map(i => {
      i[2] = Array.from(i[2]).join(', ')
      return i
    })
    await this.insertData('games', ['year', 'season', 'city'], games)

    await this.insertData('sports', ['name'], Array.from(this.sportsTable))

    await this.insertData('events', ['name'], Array.from(this.eventsTable))

    await this.insertData(
      'results',
      ['athlete_id', 'game_id', 'sport_id', 'event_id', 'medal'],
      this.resultTable
    )

    console.timeEnd('import')
  }
}

const app = new Import({ databasePath: './data/olympic_history.db' })
app.importData().then(() => {
  DB.closeDB()
})

process.on('SIGTERM', () => {
  console.info('SIGTERM signal received.')
  DB.closeDB()
})
