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

const RESULTS_COLUMNS = ['athlete_id', 'game_id', 'sport_id', 'event_id', 'medal']

const GAMES_COLUMNS = ['year', 'season', 'city']

const ATHLETES_COLUMNS = ['full_name', 'sex', 'params', 'year_of_birth', 'team_id']

const TEAMS_COLUMNS = ['noc_name', 'name']

const SPORTS_EVENTS_COLUMNS = ['name']

module.exports = {
  SEASON,
  MAX_VARIABLES,
  MEDAL,
  CURRENT_YEAR,
  RESULTS_COLUMNS,
  GAMES_COLUMNS,
  ATHLETES_COLUMNS,
  TEAMS_COLUMNS,
  SPORTS_EVENTS_COLUMNS
}
