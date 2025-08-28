# nhentai-scraper-extensions

Extensions for nhentai-scraper.

## Folder Structure
"skeleton" is a template to make extensions.

```
nhentai-scraper/
├─ [EXTENSION NAME]/
│  ├─ __init__.py   # Just needs to be here. Leave empty.
│  └─ [EXTENSION NAME]__nhsext.py   # Where the hooks for the extension live.
├─ skeleton/
│  ├─ __init__.py
│  └─ skeleton__nhsext.py
└─ master_manifest.json    # MASTER COPY OF ALL EXISTING EXTENSIONS. Pulled by the "extension_loader" module from "nhentai-scraper" and used to manage extensions. 
└─ README.md    # Thhe thing you're reading right now.
```