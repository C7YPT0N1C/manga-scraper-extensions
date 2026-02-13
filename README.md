# manga-scraper-extensions

Extensions for manga-scraper.

## Folder Structure
"skeleton" is a template to make extensions.

```
manga-scraper/
├─ [EXTENSION NAME]/
│  ├─ __init__.py   # Just needs to be here. Leave empty.
│  └─ [EXTENSION NAME]__msext.py   # Where the hooks for the extension live.
├─ skeleton/
│  ├─ __init__.py
│  └─ skeleton__msext.py
└─ master_manifest.json    # MASTER COPY OF ALL EXISTING EXTENSIONS. Pulled by the "extension_loader" module from "manga-scraper" and used to manage extensions. 
└─ README.md    # Thhe thing you're reading right now.
```