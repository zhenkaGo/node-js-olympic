const sqlite3 = require('sqlite3').verbose()

const TABLES = ['athletes', 'teams', 'games', 'sports', 'events', 'results']

class DatabaseClass {
  constructor() {
    console.log('constructor')
    this.db = new sqlite3.Database('./data/olympic_history.db', sqlite3.OPEN_READWRITE, (err) => {
      if (err) {
        console.error(err.message);
      }
      console.log('Connected to the database.')
    })
  }

  exec(...args) {
    this.db.exec(...args)
  }

  all(...args) {
    this.db.all(...args)
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
      let sql = ``
      for (const table of TABLES) {
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
}

const Database = new DatabaseClass()

module.exports = Database
