config = {
             "region": "us-west-2",
             "price": "0.300",
             "ami": "ami-ea0d62da",  # base-wdx-140304b
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
