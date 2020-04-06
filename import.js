const parser = require('./parser')
const DB = require('./database')
const {
  SEASON,
  MAX_VARIABLES,
  MEDAL,
  CURRENT_YEAR,
  RESULTS_COLUMNS,
  GAMES_COLUMNS,
  ATHLETES_COLUMNS,
  TEAMS_COLUMNS,
  SPORTS_EVENTS_COLUMNS
} = require('./constants')

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
    const step = Math.floor(MAX_VARIABLES / columns.length)
    const sqlQueries = []
    for (let i = 0; i < variables.length; i += step) {
      const valueChunk = variables.slice(i, i + step)
      const values = valueChunk.map((i) => (`(${Array.isArray(i) ? i.map(v => `"${v}"`).join(',') : `"${i}"`})`))
      sqlQueries.push(`INSERT INTO ${tableName}(${columns.join(',')}) VALUES ${values.join(',')}`)
    }
    try {
      await DB.exec(sqlQueries.join(';'))
      console.log(`${tableName} has been inserted`)
    } catch (err) {
      console.error(err, `err ========== ${tableName}`)
    }
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

  connectDB() {
    DB.connect()

    process.on('SIGTERM', () => {
      console.info('SIGTERM signal received.')
      DB.closeDB()
    })

    process.on('unhandledRejection', () => {
      console.info('unhandledRejection signal received.')
      DB.closeDB()
    })
  }

  async importData() {
    console.time('import')
    this.data = await parser('./data/athlete_events.csv')
    for (const col in this.data.columns) {
      if (this.data.columns.hasOwnProperty(col)) {
        this.columnsOfCsv.set(this.data.columns[col], col)
      }
    }
    this.connectDB()

    this.prepareDataStructure()

    await DB.clear()

    await this.insertData('teams', TEAMS_COLUMNS, Array.from(this.teamTable))

    await this.insertData('athletes', ATHLETES_COLUMNS, Array.from(this.athletesTable.values()))

    const games = Array.from(this.gamesTable.values()).map(i => {
      i[2] = Array.from(i[2]).join(', ')
      return i
    })
    await this.insertData('games', GAMES_COLUMNS, games)

    await this.insertData('sports', SPORTS_EVENTS_COLUMNS, Array.from(this.sportsTable))

    await this.insertData('events', SPORTS_EVENTS_COLUMNS, Array.from(this.eventsTable))

    await this.insertData('results', RESULTS_COLUMNS, this.resultTable)

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

process.on('unhandledRejection', () => {
  console.info('unhandledRejection signal received.')
  DB.closeDB()
})
