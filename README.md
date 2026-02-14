# manga-scraper-extensions

Extensions for manga-scraper.

## Output Format Behavior
Extensions that post-process downloads (like skeleton and suwayomi) respect `GALLERY_FORMAT`:
- `directory`: keep the original gallery folder.
- `zip`/`cbz`: archive the gallery and remove the original folder after post-processing.

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