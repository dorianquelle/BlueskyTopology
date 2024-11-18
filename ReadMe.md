# Code To Reproduce the Analysis of "Bluesky Network Topology, Polarization, and Algorithmic Curation"

## Data Preparation

1. Unzip `DIDs.txt.gz` which contains all the DIDs of users used in this analysis
2. Run the download script with:
```bash
python download_repos_multip.py --mode all
```
This will download all repositories for the DIDs and store them in the `Data/DID_REPO/` folder.

3. Run `Code/PythonScripts/data_processing.py` to create the SQL database.

## Main Analysis

### Data Processing Notebooks
* `00_CreateMBFC.ipynb` - Creates mapping of domains to political stances according to Media Bias Fact Check (MBFC)
* `01_CreateSQL.ipynb` - Creates SQL database and exports upload scripts

### Analysis Notebooks
* `02_ActivityOverTime.ipynb`
  - Figure 1: Activity over Time
  - Table 5: Top Domains

* `03_TrainTransformer.ipynb`
  - Trains model for stance detection on Israel/Palestine content

* `04_Stance.ipynb`
  - Figure 9: Proportion of Posts by Stance over Time
  - Table 1: User Activity Distributions
  - Figures 2 & 3: Distribution of Interactions
  - Figures 8 & 10: Heatmap of Ideologies vs Neighborhood Ideology
  - Figure 7: Distribution of Domains by Ideology

* `05_TopologyOverTime.ipynb`
  - Figures 4 & 5: Structural Measures over Time

* `06_Feeds.ipynb`
  - Table 2: Top Feeds on Bluesky
  - Table 4: Distributions of Feeds
  - Figure 6: Distribution of Interactions with Feeds

* `07_TopicModel.ipynb`
  - Table 3: Topic Model of Feeds

