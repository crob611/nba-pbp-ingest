import axios from "axios";
import elasticsearch from "@elastic/elasticsearch";
import * as debuglog from "axios-debug-log/enable.js";
import * as crypto from "crypto";

const uuid = crypto.randomUUID();

const LEBRON = 2544;

const NBA_HEADERS = {
  Host: "stats.nba.com",
  Connection: "keep-alive",
  Accept: "application/json",
  "User-Agent":
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.130 Safari/537.36",
  Referer: "https://stats.nba.com/",
  "x-nba-stats-origin": "stats",
  "x-nba-stats-token": "true",
  "Accept-Encoding": "gzip, deflate, br",
  "Accept-Language": "en-US,en;q=0.9",
};

const GAME_HEADERS = {
  Accept:
    "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
  "Accept-Encoding": "gzip, deflate, br",
  "Accept-Language": "en-US,en;q=0.9",
  "Cache-Control": "max-age=0",
  Connection: "keep-alive",
  Host: "cdn.nba.com",
  "User-Agent":
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36",
};

const es = new elasticsearch.Client({
  node: "http://localhost:9200",
  auth: {
    // Yeah it's my api key for my local elasticsearch instance that's probably been stopped for years now.
    // Go ahead and use it.  Or change it to your own.  Whatever.
    apiKey: "YXlCdm9uNEJ6Nzl6OTVkS1d2eVU6dWhSbDhxNjFUcEd2RWcwanpaTnJTUQ==",
  },
});

const parseGames = async () => {
  axios
    .get("https://stats.nba.com/stats/leaguegamelog", {
      headers: NBA_HEADERS,
      params: {
        Counter: 0,
        PlayerOrTeam: "T",
        Direction: "ASC",
        LeagueID: "00",
        Season: "2020-21",
        SeasonType: "Regular Season",
        Sorter: "DATE",
      },
    })
    .then(async (response) => {
      const games = response.data.resultSets[0].rowSet;
      const gameSet = new Set();

      for (let g of games) {
        gameSet.add(g[4]);
      }

      for (let g of gameSet) {
        console.log(`processing game ${g}`);
        const { data } = await axios.get(
          `https://cdn.nba.com/static/json/liveData/playbyplay/playbyplay_${g}.json`,
          {
            headers: GAME_HEADERS,
          }
        );

        const actions = data.game.actions;
        const bulkBody = [];

        await es.create({
          id: crypto.randomUUID(),
          index: "nba",
          body: actions[0],
        });

        for (let action of actions) {
          bulkBody.push({
            index: { _index: "nba" },
          });

          bulkBody.push({ gameId: g, ...action });
        }

        await es.bulk({
          body: bulkBody,
          refresh: true,
        });
      }
    });
};

parseGames();
