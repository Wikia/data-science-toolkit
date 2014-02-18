config = {
             "region": "us-west-2",
             "price": "0.300",
             "ami": "ami-fa462bca", # base-wdx-140218d
             "key": "data-extraction",
             "sec": "sshable",
             "type": "m2.4xlarge",
             "tag": "wiki_data_extraction",
             "threshold": 50,
             "max_size": 5,
             "services": [
                 "TopEntitiesService",
                 "EntityDocumentCountsService",
                 "TopHeadsService",
                 "WpTopEntitiesService",
                 "WpEntityDocumentCountsService",
                 "WikiEntitySentimentService",
                 "WpWikiEntitySentimentService",
                 "AllEntitiesSentimentAndCountsService"
                         ]
         }
