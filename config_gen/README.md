# Config Generator App

> [!NOTE]  
> 🚧 **Initial development stage – Work in Progress**  

This Streamlit app allows you to automatically generate a YAML configuration file for event and entity data.
It supports drag-and-drop CSV uploads, type inference, manual type editing, attribute selection/discarding, and YAML export.


## Features
* Upload multiple CSV files (drag & drop)
* Automatically detect the event file
* Add extra entity files manually
* Automatically infer data types (String, Integer, Datetime)
* Option to edit types and discard unwanted attributes
* Preview CSV contents before configuration
* Download ready-to-use config.yaml


## Setup & Run

(Optional) Create and activate a virtual environment

```bash
source .venv/bin/activate
```

Install dependencies

```bash
pip install streamlit pandas pyyaml
```

Run the app

```bash
streamlit run config_gen/config_gen.py
```

