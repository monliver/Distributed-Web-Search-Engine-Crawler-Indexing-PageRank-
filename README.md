# MegaSearch

This repo contains a distributed search engine with self-designed KVS system for storage and Flame service for data processing. 
Demo Video from Youtube: https://youtu.be/ObH-JEvb5gU
<img width="500" height="392" alt="Screenshot 2025-12-12 at 12 02 59 PM" src="https://github.com/user-attachments/assets/33ecb0ff-9e0c-4d09-bba1-d910ebd27116" />


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
- 

## How to use?
Core improvements (crawler/index/ranking)

1.1) Crawler robustness
We improved URL handling, robots.txt support, and crash recovery. The crawler keeps pt-crawl-queue and pt-crawl-visited consistent and can resume after failures without getting stuck.

1.2) Indexer and TF-IDF
The indexer filters stopwords and very long tokens, writes in batches, and avoids using collect() on large datasets. This helps keep memory usage under control when the crawl grows.

1.3) PageRank and scoring
We build a local link graph from pt-crawl, run iterative PageRank with a convergence check, and store the result in pt-pageranks. In the same job we compute TF-IDF for each (word, url) and build a dictionary in pt-dict that is later used by spellcheck and query suggestions.

Extra credit features and how to test them

2.1) Cached Page Viewer
Each search result has a “Cached” link.
How to test: Run any query, then click “Cached” under a result. This shows the HTML we stored when the page was crawled, even if the live site has changed.

2.2) Infinite Scrolling (“Load More Results”)
The result page shows a “Load More Results” button at the bottom. It calls /search?ajax=1&page=... and appends only the new HTML fragment.
How to test: Run a query with many results and click “Load More Results” a few times. New results appear without reloading the whole page.

2.3) Location-Based Ranking (Geo Boost)
With permission, the browser sends lat/lon. On the backend we reverse-geocode the city and slightly boost pages that mention this city in the title, URL, or snippet.
How to test: Allow location access in the browser, run a query with &diag=true, and look at the diagnostic line under each result. When the city matches, it shows something like Geo: ON (CityName), boost=....

2.4) Spellcheck (“Did you mean …?”)
We use the pt-dict table and edit distance to suggest a nearby word when the query has a small typo.
How to test: Type a query with a small spelling error that is close to a common word in our index. If we find a match, a “Did you mean …?” line appears above the results with a link to the corrected query.

2.5) Search Suggestions (Auto-complete) and Local History
As the user types, the frontend calls /suggest?prefix=... and shows completions from our dictionary. When the input box is empty, we show the last 10 queries from localStorage as history.
How to test:
– Click into the search box on the homepage or result page. If there is history, you will see entries labeled with “(History)”.
– Start typing a prefix. Suggestions appear and can be clicked to fill and submit the query.

2.6) Google Login (Firebase Authentication)
We integrated Google Sign-In using Firebase v8. The top-right panel shows “Sign in with Google”. After login, the page shows the user’s name and profile photo.
How to test: Click “Sign in with Google”, finish the popup, and check that your name and avatar appear in the top-right corner. You can also log out with the logout button.

2.7) Voice Input Search
If the browser supports the Web Speech API, the microphone button next to the search box can be used for speech-to-text.
How to test: Click the microphone button, speak a short query in English, and wait for recognition to finish. The recognized text is placed into the search box and the search runs automatically.

2.8) Document Input Search (PDF → LLM → Keywords)
The document button lets the user upload a PDF. We extract text with PDF.js, send it to the Gemini API, and ask for three meaningful keywords. These three keywords become the query.
How to test: Click the document button, upload a PDF in English, and wait for the “Processing with LLM…” overlay to finish. Then the three keywords appear in the search box and a search is triggered.

2.9) Weather Forecast for ZIP-Code Queries
On the result page, the top-right black bar is a weather ticker. It becomes active when the query is a valid 5-digit US ZIP code.
How to test: Search for a US ZIP code such as 11377. On the result page, the bar calls the OpenWeather API and shows a 3-day forecast (city name and high/low temperatures) scrolling across the bar.

