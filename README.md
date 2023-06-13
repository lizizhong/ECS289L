## ECS289L - Project3
### Work Breakdown
Dakai Kang: Select specific crop for study, screened 120/360 relevant features and prepared (cleaned, pre-processed) data for further ML-based models training pipeline

Yifeng Shi: Select econmic and political conditions’ data as the additional feature, preparing (cleaned, pre-processed) data for further advanced ML-based models training pipeline

Zizhong Li: Choose the specific ML models, establish the ML pipeline, training and evaluating models with basic feature

Zaoyi Zheng: Choose the specific ML models, establish the ML pipeline, training and evaluating models with additional advanced feature

### File Description
fetch_yield.py: fetch the yearly wheat yield in the counties of 5 states from 1993 to 2007.

fetch_lon_lat.py: fetch the longitude and latitude of the counties.

fetch_weather.py: fetch the daily weather data in the counties of 5 states from 1993 to 2007.

fetch_weather.py: fetch the soil data in the counties of 5 states.

weekly_weather.py: convert daily weather data to weekly weather data

monthly_weather.py: convert daily weather data to monthly weather data

File Advanced Features.ipynb: used for adding political and economic features into the dataset. We obtained the ruling parties data from searching online, and state-level GDP data from BEA.gov. Converted these data to dict in Python for integration.

BasicML.ipynb: The pipeline of ML models we used in the basic part, including the training and evaluate sections.
