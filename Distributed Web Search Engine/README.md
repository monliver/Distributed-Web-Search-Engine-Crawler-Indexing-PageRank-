# MegaSearch

This repo contains a distributed search engine with self-designed KVS system for storage and Flame service for data processing. 

## Build & Run (via build.sh)
We have developed an all-in-one deployment service -- `ci_cd/build.sh` for 
1. compiling class files
2. building jar file
3. deploying files to EC2
4. starting services including KVS, Flame, webapp
5. executing jobs including crawler, indexer, pageranks (can execute all three in one trigger)

```zsh
# Build
chmod +x ci_cd/build.sh # first time only to grant permission
ci_cd/build.sh build

# Start all services (requires WORKER_COUNT)
export WORKER_COUNT=3
ci_cd/build.sh start

# Submit jobs
ci_cd/build.sh crawler
ci_cd/build.sh indexer
ci_cd/build.sh pagerank

# or run this once for automating the jobs above
ci_cd/build.sh pipeline

# Web app
sudo ci_cd/build.sh webapp 8080

# Status / Stop
ci_cd/build.sh status
ci_cd/build.sh stop  # stop all services excluding webapp
```

## System Architecture

```
Browser (client)  →  WebApp :8080  →  KVS cluster (coordinator + workers)
                                            ↑
                                  Flame cluster (coordinator + workers)
                                     ↑             ↑            ↑
                                     |             |            |
                                 jobs.Crawler  jobs.Indexer  jobs.PageRank

```

## Components Overview

- `kvs.Coordinator` (`8000`): KVS cluster coordinator
- `kvs.Worker` (`8001+`): KVS storage workers
- `flame.Coordinator` (`9000`): Flame job coordinator
- `flame.Worker` (`9001+`): Flame compute workers
- `frontend.app.WebApp` (`8080`): HTTP web application serving search UI and results

Database tables:
- `pt-crawl`: cached pages plus fetch metadata.
- `pt-crawl-queue`: pending crawl frontier.
- `pt-crawl-visited`: dedup ledger for seen URLs.
- `pt-index`: chunked inverted index (`chunkNNNN`, `__count`).
- `pt-index-progress`: indexer resume markers.
- `pt-pagerank-graph`: PageRank adjacency + metadata.
- `pt-pageranks`: final scores + snippets served to UI.
- `pt-pagerank-progress`: PageRank resume state.
- `pt-tfidf`: per word/url TF-IDF vectors.
- `pt-tfidf-progress`: TF-IDF job progress markers.

## Approved Third-Party Components

- Backend
    - We only use the Java standard library plus the KVS and Flame code from CIS5550.
    - There are no external backend libraries, no external databases, and no cloud services involved in crawler, indexer, or PageRank.

- Frontend (browser-only APIs / services):
    - These are used only for extra-credit features. They do not affect the backend system.
    - Google Sign-In (Firebase Authentication)
        - Used for Google login on the search page. After login, we show the user’s name and profile photo. The search engine still works normally without login.
    - Web Speech API
        - Used for the microphone button and voice input.
    - PDF.js
        - Used to extract text from PDF files in the browser so we can build a search query.
        - We do not store PDF content in KVS.
    - Google Generative Language API (Gemini)
        - Called after PDF text extraction to get three keywords.
        - The response is used only to form the query.
    - OpenWeather API
        - Used when a user types a valid U.S. ZIP code.
        - We show a 3-day forecast in a small scrolling weather bar.
    - OpenStreetMap Nominatim
        - Used for reverse-geocoding the user’s latitude/longitude to a city name for geo-boost ranking.




## Performance Highlights

- Resume-aware crawler, indexer, PageRank, and TF-IDF pipelines (progress tables keep reruns incremental).
- Cached page viewer exposes crawler snapshots even if live pages move.
- Geo-aware ranking adds lightweight city boosts from browser latitude/longitude.
- AJAX “Load More” keeps result pages light while supporting large result sets.
- Spellcheck, autocomplete, and local history smooth query input.
- Voice search, Google login, PDF-to-query, and weather overlay showcase richer UI hooks.


## More about `build.sh`

- Environment detection and safe defaults (ports, logs, pids).
- Cleaning and compiling Java sources; packaging `lib/megasearch.jar` with static assets.
- Starting coordinators/workers, waiting for ports, and PID management.
- Job submission wiring to Flame coordinator.
- Web app launch and basic port validation.
- Optional fresh clean and database wipe via flags.
