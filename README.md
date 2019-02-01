# Behavioral Based Customer-Segmentation

**Currently working on this project**

This project uses KMeans clustering to build behavioral based customer segments using RFE (Recency, Frequency, Engagement) analysis. RFE analysis is a variation of the RFM (Recency, Frequency, Monetary) marketing model used to quantify customer behavior. The framework works by grouping customers based on how recently a customer has purchased (recency), how often (frequency), and by how much (monetary). For the particular dataset used in this project, recency is defined as the last time a user clicked on an ad, frequency as the number of total ad clicks, and engagement as the total number of pages visited.  Aftering defining customer segments, it allows businesses to answer questions such as:

1.) Who are your loyal customers?

2.) Which customers have the potential to be converted to more profitable customers?

3.) Which is the best method to reach out to a particular customer?

4.) Which customers are not worth allocating resources to?


### Dataset
The dataset used for this project contains click information on ads from users navigating the website Avito: Russia's largest website for classified ads. The data is contained in a 80gb sqlite database with a total of 8 relational tables. The tables hold information on users previous searches (search date, query entered, location, historical CTR) as well as metadata on the ads shown (title, category, price). Only two tables in particular were needed in this project. One being `trainSearchStream` which is a random sample of previously selected users' searches on Avito during at least 16 consecutive days from April'25 until some target impression date. The second being `SearchInfo` which contains additional information on a user's search session. More on the dataset can he found [here](https://www.kaggle.com/c/avito-context-ad-clicks/data).

### Data Preprocessing
Three features were computed inorder to conduct the analysis:

1.) The number of days a user was last seen.

2.) The total number of clicks for a user.

3.) The total number of sessions for a user.

To calculate the number of days a user was last seen, the latest search date in the dataset was taken plus one day and subtracted by the 
last search date for each user. 

To calculate the total number of session and clicks for a user, all that is needed is a simple aggregate function to sum the total clicks and count of user ids.

The code for computing these features is found in `get_rfe.py`

### Data Pipeline
To manage the memory and compute intensive tasks from the large amount of data, a data pipeline was built and deployed on AWS. Namely, S3 was used for storage and an EMR Spark cluster was used for parallel processing of tasks.

### Modeling
The K-means algorithm was used as it is a popular unsupervised machine learning algorithm used to perform clustering and segmentation tasks. As a brief overview, K-means groups similar data points together and looks for a fixed number (k) of clusters in the data. It does this by first randomly generating k number of centroids to initialize the clusters. Then goes through a number of iterations where each data point is assigned to its nearest centroid based on the squared Euclidean distance. Next, the centroids are updated by computing the mean of all data points assigned to that centroid's cluster. The algorithm stops when the sum of distances are minimized or when a max number of iterations are reached.     


### Results
The diagrams below show the results after running K-means on the dataset using RFE as features.



