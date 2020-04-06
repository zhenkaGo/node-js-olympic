const sqlite3 = require('sqlite3').verbose()
const util = require('util')

const TABLES = ['athletes', 'teams', 'games', 'sports', 'events', 'results']

class DatabaseClass {
  connect() {
    this.db = new sqlite3.Database('./data/olympic_history.db', sqlite3.OPEN_READWRITE, err => {
      if (err) {
        return console.error(err.message)
      }
      console.log('Connected to the database.')
    })
  }

  exec(sqlQuery) {
    const exec = util.promisify(this.db.exec.bind(this.db))
    return exec(sqlQuery)
  }

  all(sqlQuery) {
    const all = util.promisify(this.db.all.bind(this.db))
    return all(sqlQuery)
  }

  closeDB() {
    const close = util.promisify(this.db.close.bind(this.db))
    try {
      close()
      console.log(`Database has been closed`)
    } catch (err) {
      return console.error(err, 'closeDB error ==========')
    }
  }

  async clear() {
    try {
      let sql = ``
      for (const table of TABLES) {
        sql = `${sql} DELETE FROM ${table}; VACUUM; DELETE FROM sqlite_sequence WHERE name = '${table}';`
      }
      await this.exec(sql)
      console.log(`Tables has been cleared`)
    } catch (err) {
      console.error(err, 'clear tables error =============')
    }
  }
}

const Database = new DatabaseClass()
module.exports = Database
