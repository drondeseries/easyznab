# Easyznab

Easyznab is a high-performance Flask/Gunicorn bridge server that connects Easynews searches to a Newznab/Torznab-compatible XML RSS API. This allows you to add Easynews as a custom indexer in **Prowlarr**, **Sonarr**, and **Radarr** to search, index, and download NZB files.

## ✨ Features

- 🌐 **Newznab/Torznab RSS Bridge**: Exposes a standard Usenet indexer interface `/api` and `/v2/api` for Prowlarr/Sonarr/Radarr.
- ⚡ **Shared Memory Cache**: Built with Gunicorn `gthread` worker architecture, sharing the in-memory search caches across all incoming concurrent queries.
- 🔒 **Concurrency & Rate Limit Protection**: Uses a global thread-safe request semaphore to throttle concurrent Solr requests to Easynews, preventing upstream rate limits and empty (`char 0`) search responses.
- 📂 **Smart Archive Handling**: Automatically groups split RAR releases, checks for pre-existing uploaded NZB files directly on Easynews, and falls back to building functional NZBs on-the-fly.
- 🧼 **Clean Results**: Automatically filters out samples, duplicates, and viruses from search results.
- 🔄 **Dynamic Pagination**: Automatically paginates and fetches multiple pages (up to a configurable limit) when search queries are saturated (1,000+ matches), ensuring older content (like season 1 of long-running shows) isn't missed.
- 🛡️ **TRaSH Guides-Style Filtering**: Built-in support to automatically discard low-quality, unwanted release types (CAM, Telesync, Telecine, Screeners, 3D, and hardcoded Korean subtitles) with smart bypass safety nets for explicit search queries.

---

## 🚀 Setup & Self-Hosting

### 1. Configure the Environment
Create a `.env` file in the application directory:

```bash
EASYNEWS_USER=your_easynews_username
EASYNEWS_PASS=your_easynews_password

# API key used by Prowlarr/Newznab clients to authenticate to this server
# In production, change this to a secure random token!
NEWZNAB_APIKEY=generate_a_secure_random_key_here

# Whether to allow password-protected releases (default: true)
ALLOW_PASSWORDED=true

# --- ADVANCED CONFIGURATION ---

# Maximum number of Solr search pages to fetch for saturated queries (default: 5)
# EASYNEWS_MAX_PAGES=5

# Discard low-quality release types (CAM, TS, TC, Screener, 3D, Korsub) (default: true)
# DISCARD_LOW_QUALITY=true

# Custom regex to exclude unwanted release titles (case-insensitive)
# EASYNEWS_EXCLUDE_REGEX=\b(?:my_custom_blacklist_word)\b

# Optional: override server port (default 8080)
# PORT=8080
```

### 2. Configure Custom Titles (Optional)
If you want to map alternate titles or deobfuscate specific release hashes, edit the local `custom-titles.json` file and uncomment the volumes block in your `docker-compose.yml` file:
```yaml
    volumes:
      - ./custom-titles.json:/app/custom-titles.json
```

### 3. Deploy via Docker Compose (Recommended)
Launch the application:

```bash
docker compose up -d
```

The server will pull the prebuilt image from GHCR and start listening on port `8080`.

---

## 🛠️ API Versions (v1 vs v2)

Easyznab exposes two API endpoints that govern how password-protected archive files are handled:

### **v1 API** (`/api` or `/`)
- **Behavior**: Filters out and discards all password-protected archive releases (`allow_password_archives = False`).
- **Use Case**: 100% safe from compressed RAR failures. Recommended if you want maximum compatibility with standard downloaders that cannot decrypt or handle passworded archives.

### **v2 API** (`/v2/api` or `/api/v2`)
- **Behavior**: Allows password-protected archives (`allow_password_archives = True`).
- **Use Case**: Recommended when integrating with Usenet downloaders or streaming mounts that support processing and decrypting uncompressed password-protected RAR archives.

---

## ☕ Credits & Attribution

- Built upon the foundation of **[Easynews_as_indexer](https://github.com/Sanket9225/Easynews_as_indexer)** developed by **[Sanket Gaikwad](https://github.com/Sanket9225)**. Special thanks for their awesome work creating the original Flask bridge!
- Matching logic, deobfuscation, and query expansion designs were inspired by **[easynews-plus-plus](https://github.com/Varming73/easynews-plus-plus)** (originally by **[panteLx](https://github.com/panteLx)**, maintained by **[Varming73](https://github.com/Varming73)** and **[Sleeyax](https://github.com/Sleeyax)**).
